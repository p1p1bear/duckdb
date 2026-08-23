//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/allocator_block_reservation.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

class BlockHandle;
class BlockManager;

//! Prevents future allocator reuse without changing persistent block reference counts.
//! The caller must already exclude earlier allocator claims; registering multiple blocks is not set-atomic.
//! Physical blocks must be validated as fully written before they are read or retained across a checkpoint.
class AllocatorBlockReservation {
public:
	AllocatorBlockReservation() = default;
	AllocatorBlockReservation(AllocatorBlockReservation &&other) noexcept;
	AllocatorBlockReservation &operator=(AllocatorBlockReservation &&other) noexcept;
	~AllocatorBlockReservation();

	AllocatorBlockReservation(const AllocatorBlockReservation &) = delete;
	AllocatorBlockReservation &operator=(const AllocatorBlockReservation &) = delete;

	static AllocatorBlockReservation Reserve(BlockManager &block_manager, const vector<block_id_t> &physical_blocks);
	void AddPhysicalBlock(block_id_t block_id);
	bool Covers(const vector<block_id_t> &all_resources) const;
	void Release() noexcept;

private:
	struct Entry {
		block_id_t block_id;
		shared_ptr<BlockHandle> handle;
	};

	explicit AllocatorBlockReservation(BlockManager &block_manager);

private:
	optional_ptr<BlockManager> block_manager;
	vector<Entry> entries;
};

} // namespace duckdb
