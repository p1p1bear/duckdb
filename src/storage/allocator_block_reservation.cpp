#include "duckdb/storage/allocator_block_reservation.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

AllocatorBlockReservation::AllocatorBlockReservation(BlockManager &block_manager_p) : block_manager(block_manager_p) {
}

AllocatorBlockReservation::AllocatorBlockReservation(AllocatorBlockReservation &&other) noexcept
    : block_manager(other.block_manager), entries(std::move(other.entries)) {
	other.block_manager = nullptr;
}

AllocatorBlockReservation &AllocatorBlockReservation::operator=(AllocatorBlockReservation &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	Release();
	block_manager = other.block_manager;
	entries = std::move(other.entries);
	other.block_manager = nullptr;
	return *this;
}

AllocatorBlockReservation::~AllocatorBlockReservation() {
	Release();
}

AllocatorBlockReservation AllocatorBlockReservation::Reserve(BlockManager &block_manager,
                                                             const vector<block_id_t> &physical_blocks) {
	auto blocks = physical_blocks;
	std::sort(blocks.begin(), blocks.end());
	blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());

	AllocatorBlockReservation result(block_manager);
	result.entries.reserve(blocks.size());
	for (auto block_id : blocks) {
		result.AddPhysicalBlock(block_id);
	}
	return result;
}

void AllocatorBlockReservation::AddPhysicalBlock(block_id_t block_id) {
	if (!block_manager) {
		throw InternalException("Cannot extend an inactive allocator block reservation");
	}
	if (block_id < 0) {
		throw InternalException("Cannot reserve invalid physical block ID %d", block_id);
	}
	auto position = std::lower_bound(entries.begin(), entries.end(), block_id,
	                                 [](const Entry &entry, block_id_t value) { return entry.block_id < value; });
	if (position != entries.end() && position->block_id == block_id) {
		return;
	}
	auto handle = block_manager->RegisterBlockReservation(block_id);
	try {
		entries.insert(position, Entry {block_id, handle});
	} catch (...) {
		block_manager->UnregisterBlockReservation(block_id);
		handle.reset();
		throw;
	}
}

bool AllocatorBlockReservation::Covers(const vector<block_id_t> &all_resources) const {
	for (auto block_id : all_resources) {
		auto position = std::lower_bound(entries.begin(), entries.end(), block_id,
		                                 [](const Entry &entry, block_id_t value) { return entry.block_id < value; });
		if (position == entries.end() || position->block_id != block_id) {
			return false;
		}
	}
	return true;
}

bool AllocatorBlockReservation::Covers(const BlockManager &expected_manager,
                                       const vector<block_id_t> &all_resources) const {
	return IsActiveFor(expected_manager) && Covers(all_resources);
}

bool AllocatorBlockReservation::IsActiveFor(const BlockManager &expected_manager) const noexcept {
	return block_manager.get() == &expected_manager;
}

void AllocatorBlockReservation::Release() noexcept {
	if (!block_manager) {
		return;
	}
	for (auto &entry : entries) {
		block_manager->UnregisterBlockReservation(entry.block_id);
	}
	entries.clear();
	block_manager = nullptr;
}

} // namespace duckdb
