#include "duckdb/storage/recluster/recluster_wal_retention.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/allocator_block_reservation.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

struct ReclusterWALRetentionNode {
	AllocatorBlockReservation block_reservation;
	ReclusterWALPosition transaction_end;
	unique_ptr<ReclusterWALRetentionNode> next;
	bool retain_until_shutdown = false;
};

ReclusterWALRetentionReservation::ReclusterWALRetentionReservation() {
}

ReclusterWALRetentionReservation::ReclusterWALRetentionReservation(unique_ptr<ReclusterWALRetentionNode> node_p)
    : node(std::move(node_p)) {
}

ReclusterWALRetentionReservation::ReclusterWALRetentionReservation(ReclusterWALRetentionReservation &&other) noexcept =
    default;

ReclusterWALRetentionReservation &
ReclusterWALRetentionReservation::operator=(ReclusterWALRetentionReservation &&other) noexcept = default;

ReclusterWALRetentionReservation::~ReclusterWALRetentionReservation() {
}

bool ReclusterWALRetentionReservation::IsActive() const {
	return node != nullptr;
}

ReclusterWALBlockRetention::ReclusterWALBlockRetention(AttachedDatabase &db_p) : db(db_p) {
}

ReclusterWALBlockRetention::~ReclusterWALBlockRetention() {
	DestroyList(std::move(entries));
}

ReclusterWALRetentionReservation ReclusterWALBlockRetention::Reserve(vector<block_id_t> manifest_chain,
                                                                     vector<block_id_t> replay_required_blocks) {
	manifest_chain.insert(manifest_chain.end(), replay_required_blocks.begin(), replay_required_blocks.end());
	std::sort(manifest_chain.begin(), manifest_chain.end());
	manifest_chain.erase(std::unique(manifest_chain.begin(), manifest_chain.end()), manifest_chain.end());
	if (manifest_chain.empty()) {
		throw InternalException("Cannot create an empty recluster WAL retention reservation");
	}

	auto node = make_uniq<ReclusterWALRetentionNode>();
	auto &block_manager = db.GetStorageManager().GetBlockManager();
	node->block_reservation = AllocatorBlockReservation::Reserve(block_manager, manifest_chain);
	return ReclusterWALRetentionReservation(std::move(node));
}

void ReclusterWALBlockRetention::Commit(ReclusterWALRetentionReservation &&reservation,
                                        ReclusterWALPosition transaction_end) noexcept {
	D_ASSERT(reservation.node);
	if (!reservation.node) {
		return;
	}
	D_ASSERT(transaction_end.file_offset > 0);
	auto node = std::move(reservation.node);
	node->transaction_end = transaction_end;
	lock_guard<mutex> guard(lock);
#ifdef DEBUG
	for (auto entry = entries.get(); entry; entry = entry->next.get()) {
		D_ASSERT(entry->transaction_end.checkpoint_iteration != transaction_end.checkpoint_iteration ||
		         entry->transaction_end.file_offset != transaction_end.file_offset);
	}
#endif
	node->next = std::move(entries);
	entries = std::move(node);
}

void ReclusterWALBlockRetention::RetainUntilShutdown(ReclusterWALRetentionReservation &&reservation) noexcept {
	D_ASSERT(reservation.node);
	if (!reservation.node) {
		return;
	}
	auto node = std::move(reservation.node);
	node->retain_until_shutdown = true;
	lock_guard<mutex> guard(lock);
	node->next = std::move(entries);
	entries = std::move(node);
}

void ReclusterWALBlockRetention::ReleaseNoLongerReplayable(const WALReplayRange &remaining_wal) noexcept {
	D_ASSERT(remaining_wal.start_offset <= remaining_wal.end_offset);
	unique_ptr<ReclusterWALRetentionNode> released;
	{
		lock_guard<mutex> guard(lock);
		auto cursor = &entries;
		while (*cursor) {
			if ((*cursor)->retain_until_shutdown) {
				cursor = &(*cursor)->next;
				continue;
			}
			auto &position = (*cursor)->transaction_end;
			if (position.checkpoint_iteration < remaining_wal.checkpoint_iteration) {
				auto removed = std::move(*cursor);
				*cursor = std::move(removed->next);
				removed->next = std::move(released);
				released = std::move(removed);
				continue;
			}
			if (position.checkpoint_iteration == remaining_wal.checkpoint_iteration) {
				D_ASSERT(remaining_wal.Contains(position));
			} else {
				D_ASSERT(false);
			}
			cursor = &(*cursor)->next;
		}
	}
	DestroyList(std::move(released));
}

idx_t ReclusterWALBlockRetention::Count() const {
	lock_guard<mutex> guard(lock);
	idx_t count = 0;
	for (auto entry = entries.get(); entry; entry = entry->next.get()) {
		count++;
	}
	return count;
}

void ReclusterWALBlockRetention::DestroyList(unique_ptr<ReclusterWALRetentionNode> list) noexcept {
	while (list) {
		auto current = std::move(list);
		list = std::move(current->next);
	}
}

} // namespace duckdb
