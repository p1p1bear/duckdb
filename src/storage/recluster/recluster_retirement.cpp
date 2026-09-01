#include "duckdb/storage/recluster/recluster_retirement.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/storage/allocator_block_reservation.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_drop_ownership_runtime.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"

namespace duckdb {

enum class ReclusterRetirementState : uint8_t { WAITING, CLAIMING, DROP_PERSISTED, POISONED };

struct ReclusterRetirementEntry {
	shared_ptr<RowGroupCollection> collection;
	weak_ptr<const RowGroupLayout> reader_layout;
	vector<shared_ptr<RowGroupColumnDropOwnership>> ownership;
	vector<block_id_t> protected_resources;
	AllocatorBlockReservation reservation;
	ReclusterRetirementState state = ReclusterRetirementState::WAITING;
	unique_ptr<ReclusterRetirementEntry> next;
};

enum class ReclusterRetirementCheckpointPhase : uint8_t { PREPARED, APPLIED, SUCCEEDED };

struct ReclusterRetirementCheckpointState {
	explicit ReclusterRetirementCheckpointState(ReclusterRetirementRegistry &registry_p) : registry(registry_p) {
	}

	ReclusterRetirementRegistry &registry;
	vector<reference<ReclusterRetirementEntry>> entries;
	vector<vector<block_id_t>> source_actions;
	shared_ptr<RowGroupColumnDropClaim> claim_owner;
	vector<shared_ptr<RowGroupColumnDropOwnership>> claimed_ownership;
	unique_lock<mutex> ownership_lock;
	unique_ptr<PreparedCheckpointBlockDropBatch> block_batch;
	ReclusterRetirementCheckpointPhase phase = ReclusterRetirementCheckpointPhase::PREPARED;
};

PreparedReclusterRetirement::PreparedReclusterRetirement() {
}

PreparedReclusterRetirement::PreparedReclusterRetirement(PreparedReclusterRetirement &&other) noexcept = default;

PreparedReclusterRetirement &
PreparedReclusterRetirement::operator=(PreparedReclusterRetirement &&other) noexcept = default;

PreparedReclusterRetirement::~PreparedReclusterRetirement() {
}

PreparedReclusterRetirement::PreparedReclusterRetirement(unique_ptr<ReclusterRetirementEntry> entry_p)
    : entry(std::move(entry_p)) {
}

bool PreparedReclusterRetirement::IsActive() const noexcept {
	return entry != nullptr;
}

ReclusterRetirementCheckpoint::ReclusterRetirementCheckpoint() {
}

ReclusterRetirementCheckpoint::ReclusterRetirementCheckpoint(ReclusterRetirementCheckpoint &&other) noexcept = default;

ReclusterRetirementCheckpoint::~ReclusterRetirementCheckpoint() {
	if (state && state->phase != ReclusterRetirementCheckpointPhase::SUCCEEDED) {
		state->registry.AbortCheckpoint(*state);
	}
}

ReclusterRetirementCheckpoint::ReclusterRetirementCheckpoint(unique_ptr<ReclusterRetirementCheckpointState> state_p)
    : state(std::move(state_p)) {
}

bool ReclusterRetirementCheckpoint::Empty() const noexcept {
	return !state || state->entries.empty();
}

void ReclusterRetirementCheckpoint::Apply() noexcept {
	if (!state || state->phase != ReclusterRetirementCheckpointPhase::PREPARED) {
		D_ASSERT(false);
		return;
	}
	if (state->block_batch) {
		auto &block_manager = state->entries[0].get().collection->GetBlockManager().Cast<SingleFileBlockManager>();
		block_manager.ApplyPreparedBlockDrops(std::move(*state->block_batch));
		state->block_batch.reset();
	}
	state->phase = ReclusterRetirementCheckpointPhase::APPLIED;
}

void ReclusterRetirementCheckpoint::HeaderSucceeded() noexcept {
	if (!state || state->phase != ReclusterRetirementCheckpointPhase::APPLIED) {
		D_ASSERT(false);
		return;
	}
	state->registry.FinishCheckpoint(*state);
	state->phase = ReclusterRetirementCheckpointPhase::SUCCEEDED;
}

class RetirementBlockCollector : public BlockIdVisitor {
public:
	void Visit(block_id_t block_id) override {
		blocks.push_back(block_id);
	}

	vector<block_id_t> blocks;
};

static void AddMetadataPointer(vector<block_id_t> &blocks, const MetaBlockPointer &pointer) {
	if (pointer.IsValid()) {
		blocks.push_back(pointer.GetBlockId());
	}
}

struct RetirementOwnershipSource {
	shared_ptr<RowGroupColumnDropOwnership> ownership;
	vector<block_id_t> actions;
};

static void CollectRowGroupRetirement(RowGroup &row_group, vector<RetirementOwnershipSource> &ownership_sources,
                                      reference_map_t<RowGroupColumnDropOwnership, idx_t> &ownership_indexes,
                                      vector<block_id_t> &resources) {
	for (idx_t column_index = 0; column_index < row_group.GetColumnCount(); column_index++) {
		row_group.GetColumnDropOwnershipBundle(column_index);
		auto tree = CaptureColumnDropOwnershipRuntimeTree(row_group.GetRawColumnData(column_index));
		for (auto &node_ref : tree.nodes) {
			auto &node = node_ref.get();
			auto ownership = node.GetDropOwnershipToken();
			if (!ownership) {
				throw InternalException("Cannot retire a column without initialized block ownership");
			}
			RetirementBlockCollector collector;
			node.VisitDirectBlockIds(collector);
			resources.insert(resources.end(), collector.blocks.begin(), collector.blocks.end());

			auto existing = ownership_indexes.find(*ownership);
			if (existing != ownership_indexes.end()) {
				if (ownership_sources[existing->second].actions != collector.blocks) {
					throw InternalException("Conflicting actions for shared retirement ownership");
				}
				continue;
			}
			ownership_indexes.emplace(*ownership, ownership_sources.size());
			ownership_sources.push_back({std::move(ownership), std::move(collector.blocks)});
		}
	}

	for (auto &pointer : row_group.GetColumnStartPointers()) {
		AddMetadataPointer(resources, pointer);
	}
	for (auto &pointer : row_group.GetExtraMetadataBlockPointers()) {
		AddMetadataPointer(resources, pointer);
	}
	for (auto &pointer : row_group.GetDeleteStartPointers()) {
		AddMetadataPointer(resources, pointer);
	}
	for (auto &pointer : row_group.GetLoadedDeleteStoragePointers()) {
		AddMetadataPointer(resources, pointer);
	}
}

ReclusterRetirementRegistry::ReclusterRetirementRegistry(AttachedDatabase &db_p) : db(db_p) {
}

ReclusterRetirementRegistry::~ReclusterRetirementRegistry() {
	DestroyEntries(std::move(entries));
}

PreparedReclusterRetirement
ReclusterRetirementRegistry::PrepareLayoutRetirement(shared_ptr<RowGroupCollection> collection,
                                                     shared_ptr<const RowGroupLayout> old_layout,
                                                     RowGroupRange retired_range) {
	if (!collection || !old_layout || retired_range.start < 0 || retired_range.start >= retired_range.end) {
		throw InternalException("Invalid recluster layout retirement request");
	}
	if (&collection->GetBlockManager() != &db.GetStorageManager().GetBlockManager()) {
		throw InternalException("Cannot retire a layout through a different attached database");
	}

	vector<RetirementOwnershipSource> ownership_sources;
	reference_map_t<RowGroupColumnDropOwnership, idx_t> ownership_indexes;
	vector<block_id_t> resources;
	LayoutRowGroupCursor cursor(RowGroupCollectionSnapshot(old_layout), retired_range);
	LayoutRowGroupEntry current;
	idx_t retired_row_group_count = 0;
	while (cursor.Next(current)) {
		CollectRowGroupRetirement(*current.row_group, ownership_sources, ownership_indexes, resources);
		retired_row_group_count++;
	}
	if (retired_row_group_count == 0) {
		throw InternalException("Recluster layout retirement range contains no row groups");
	}

	auto &block_manager = collection->GetBlockManager();
	vector<shared_ptr<RowGroupColumnDropOwnership>> ownership;
	ownership.reserve(ownership_sources.size());
	std::sort(resources.begin(), resources.end());
	resources.erase(std::unique(resources.begin(), resources.end()), resources.end());

	auto entry = make_uniq<ReclusterRetirementEntry>();
	entry->collection = std::move(collection);
	entry->reader_layout = std::move(old_layout);
	entry->protected_resources = std::move(resources);
	{
		auto ownership_lock = block_manager.LockOwnershipDrops();
		for (auto &source : ownership_sources) {
			source.ownership->FreezeOrVerify(std::move(source.actions));
			ownership.push_back(std::move(source.ownership));
		}
		entry->reservation = AllocatorBlockReservation::Reserve(block_manager, entry->protected_resources);
	}
	entry->ownership = std::move(ownership);
	return PreparedReclusterRetirement(std::move(entry));
}

void ReclusterRetirementRegistry::Commit(PreparedReclusterRetirement &&retirement) noexcept {
	D_ASSERT(retirement.entry);
	if (!retirement.entry) {
		return;
	}
	auto entry = std::move(retirement.entry);
	lock_guard<mutex> guard(lock);
	entry->next = std::move(entries);
	entries = std::move(entry);
}

ReclusterRetirementCheckpoint ReclusterRetirementRegistry::PrepareCheckpoint() {
	auto checkpoint = make_uniq<ReclusterRetirementCheckpointState>(*this);
	try {
		{
			lock_guard<mutex> guard(lock);
			idx_t waiting_count = 0;
			for (auto entry = entries.get(); entry; entry = entry->next.get()) {
				if (entry->state == ReclusterRetirementState::WAITING) {
					waiting_count++;
				}
			}
			checkpoint->entries.reserve(waiting_count);
			for (auto entry = entries.get(); entry; entry = entry->next.get()) {
				if (entry->state != ReclusterRetirementState::WAITING) {
					continue;
				}
				entry->state = ReclusterRetirementState::CLAIMING;
				checkpoint->entries.emplace_back(*entry);
			}
		}
		if (checkpoint->entries.empty()) {
			return ReclusterRetirementCheckpoint(std::move(checkpoint));
		}

		auto &block_manager = db.GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
		checkpoint->source_actions.resize(checkpoint->entries.size());
		checkpoint->claim_owner = make_shared_ptr<RowGroupColumnDropClaim>();
		idx_t ownership_count = 0;
		for (auto &entry_ref : checkpoint->entries) {
			ownership_count += entry_ref.get().ownership.size();
		}
		checkpoint->claimed_ownership.reserve(ownership_count);
		reference_set_t<RowGroupColumnDropOwnership> visited;
		checkpoint->ownership_lock = block_manager.LockOwnershipDrops();
		for (idx_t entry_index = 0; entry_index < checkpoint->entries.size(); entry_index++) {
			auto &entry = checkpoint->entries[entry_index].get();
			for (auto &ownership : entry.ownership) {
				if (!visited.insert(reference<RowGroupColumnDropOwnership>(*ownership)).second) {
					continue;
				}
				if (!ownership->Claim(checkpoint->claim_owner)) {
					if (ownership->GetState() != RowGroupColumnDropOwnership::State::DROPPED) {
						throw InternalException("Unexpected retirement ownership claim result");
					}
					continue;
				}
				checkpoint->claimed_ownership.push_back(ownership);
				auto &actions = ownership->GetActions();
				checkpoint->source_actions[entry_index].insert(checkpoint->source_actions[entry_index].end(),
				                                               actions.begin(), actions.end());
			}
		}

		vector<CheckpointBlockDropSource> sources;
		sources.reserve(checkpoint->entries.size());
		for (idx_t entry_index = 0; entry_index < checkpoint->entries.size(); entry_index++) {
			auto &entry = checkpoint->entries[entry_index].get();
			sources.emplace_back(checkpoint->source_actions[entry_index], entry.protected_resources, entry.reservation);
		}
		auto batch = block_manager.PrepareBlockDropsForCheckpoint(sources);
		checkpoint->block_batch = make_uniq<PreparedCheckpointBlockDropBatch>(std::move(batch));
	} catch (...) {
		AbortCheckpoint(*checkpoint);
		throw;
	}
	return ReclusterRetirementCheckpoint(std::move(checkpoint));
}

void ReclusterRetirementRegistry::AbortCheckpoint(ReclusterRetirementCheckpointState &checkpoint) noexcept {
	checkpoint.block_batch.reset();
	if (checkpoint.claim_owner) {
		for (auto &ownership : checkpoint.claimed_ownership) {
			bool transitioned;
			if (checkpoint.phase == ReclusterRetirementCheckpointPhase::APPLIED) {
				transitioned = ownership->Poison(*checkpoint.claim_owner);
			} else {
				transitioned = ownership->Revert(*checkpoint.claim_owner);
			}
			D_ASSERT(transitioned);
			(void)transitioned;
		}
	}
	if (checkpoint.ownership_lock.owns_lock()) {
		checkpoint.ownership_lock.unlock();
	}
	{
		lock_guard<mutex> guard(lock);
		for (auto &entry_ref : checkpoint.entries) {
			auto &entry = entry_ref.get();
			entry.state = checkpoint.phase == ReclusterRetirementCheckpointPhase::APPLIED
			                  ? ReclusterRetirementState::POISONED
			                  : ReclusterRetirementState::WAITING;
		}
	}
	if (checkpoint.phase == ReclusterRetirementCheckpointPhase::APPLIED) {
		try {
			ValidChecker::Invalidate(db.GetDatabase(),
			                         "Checkpoint failed after applying recluster retirement block drops");
		} catch (...) { // NOLINT: checkpoint cleanup cannot report a second failure
		}
	}
}

void ReclusterRetirementRegistry::FinishCheckpoint(ReclusterRetirementCheckpointState &checkpoint) noexcept {
	for (auto &ownership : checkpoint.claimed_ownership) {
		auto finalized = ownership->Finalize(*checkpoint.claim_owner);
		D_ASSERT(finalized);
		(void)finalized;
	}
	if (checkpoint.ownership_lock.owns_lock()) {
		checkpoint.ownership_lock.unlock();
	}
	{
		lock_guard<mutex> guard(lock);
		for (auto &entry_ref : checkpoint.entries) {
			entry_ref.get().state = ReclusterRetirementState::DROP_PERSISTED;
		}
	}
	Cleanup();
}

void ReclusterRetirementRegistry::Cleanup() noexcept {
	unique_ptr<ReclusterRetirementEntry> released;
	{
		lock_guard<mutex> guard(lock);
		auto cursor = &entries;
		while (*cursor) {
			if ((*cursor)->state != ReclusterRetirementState::DROP_PERSISTED || !(*cursor)->reader_layout.expired()) {
				cursor = &(*cursor)->next;
				continue;
			}
			auto removed = std::move(*cursor);
			*cursor = std::move(removed->next);
			removed->next = std::move(released);
			released = std::move(removed);
		}
	}
	DestroyEntries(std::move(released));
}

idx_t ReclusterRetirementRegistry::Count() const {
	lock_guard<mutex> guard(lock);
	idx_t count = 0;
	for (auto entry = entries.get(); entry; entry = entry->next.get()) {
		count++;
	}
	return count;
}

idx_t ReclusterRetirementRegistry::GetRetiredBytes(const DataTableInfo &table_info) const {
	unordered_set<block_id_t> blocks;
	lock_guard<mutex> guard(lock);
	for (auto entry = entries.get(); entry; entry = entry->next.get()) {
		if (&entry->collection->GetTableInfo() != &table_info) {
			continue;
		}
		blocks.insert(entry->protected_resources.begin(), entry->protected_resources.end());
	}
	auto block_size = db.GetStorageManager().GetBlockManager().GetBlockAllocSize();
	if (block_size != 0 && blocks.size() > NumericLimits<idx_t>::Maximum() / block_size) {
		return NumericLimits<idx_t>::Maximum();
	}
	return blocks.size() * block_size;
}

void ReclusterRetirementRegistry::DestroyEntries(unique_ptr<ReclusterRetirementEntry> entries_p) noexcept {
	while (entries_p) {
		auto current = std::move(entries_p);
		entries_p = std::move(current->next);
	}
}

} // namespace duckdb
