//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_wal_retention.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class AttachedDatabase;
struct ReclusterWALRetentionNode;

struct ReclusterWALPosition {
	uint64_t checkpoint_iteration = 0;
	idx_t file_offset = 0;
};

struct WALReplayRange {
	uint64_t checkpoint_iteration = 0;
	idx_t start_offset = 0;
	idx_t end_offset = 0;

	bool Contains(const ReclusterWALPosition &position) const {
		return position.checkpoint_iteration == checkpoint_iteration && position.file_offset > start_offset &&
		       position.file_offset <= end_offset;
	}
};

class ReclusterWALRetentionReservation {
public:
	ReclusterWALRetentionReservation();
	ReclusterWALRetentionReservation(ReclusterWALRetentionReservation &&other) noexcept;
	ReclusterWALRetentionReservation &operator=(ReclusterWALRetentionReservation &&other) noexcept;
	~ReclusterWALRetentionReservation();

	ReclusterWALRetentionReservation(const ReclusterWALRetentionReservation &) = delete;
	ReclusterWALRetentionReservation &operator=(const ReclusterWALRetentionReservation &) = delete;

	bool IsActive() const;

private:
	friend class ReclusterWALBlockRetention;

	explicit ReclusterWALRetentionReservation(unique_ptr<ReclusterWALRetentionNode> node);

private:
	unique_ptr<ReclusterWALRetentionNode> node;
};

class ReclusterWALBlockRetention {
public:
	explicit ReclusterWALBlockRetention(AttachedDatabase &db);
	~ReclusterWALBlockRetention();

	ReclusterWALRetentionReservation Reserve(vector<block_id_t> manifest_chain,
	                                         vector<block_id_t> replay_required_blocks);
	//! Commit only links a node that was fully allocated by Reserve.
	void Commit(ReclusterWALRetentionReservation &&reservation, ReclusterWALPosition transaction_end) noexcept;
	//! Release only entries from generations older than the remaining WAL.
	void ReleaseNoLongerReplayable(const WALReplayRange &remaining_wal) noexcept;

	idx_t Count() const;

private:
	static void DestroyList(unique_ptr<ReclusterWALRetentionNode> list) noexcept;

private:
	AttachedDatabase &db;
	mutable mutex lock;
	unique_ptr<ReclusterWALRetentionNode> entries;
};

} // namespace duckdb
