//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_commit.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/recluster_retirement.hpp"
#include "duckdb/storage/recluster/recluster_wal_retention.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class CommitDropState;
class DataTable;
class RangeTask;
class RowGroupLayout;
class TableReclusterState;
class WriteAheadLog;
struct PreparedReclusterWAL;

struct PreparedReclusterCommitResources {
	PreparedReclusterRetirement retirement;
	ReclusterWALRetentionReservation wal_retention;
	uint64_t wal_checkpoint_iteration = 0;
};

class ReclusterCommitInfo {
public:
	static PreparedReclusterCommitResources Prepare(const shared_ptr<RangeTask> &task,
	                                                const shared_ptr<DataTable> &storage,
	                                                const shared_ptr<const RowGroupLayout> &old_layout);
	ReclusterCommitInfo(shared_ptr<RangeTask> task, shared_ptr<TableReclusterState> table_state,
	                    shared_ptr<DataTable> storage, shared_ptr<const RowGroupLayout> old_layout,
	                    shared_ptr<RowGroupLayout> pending_layout, vector<row_t> final_deleted_new_rowids,
	                    delete_sequence_t journal_resolved_through, PreparedReclusterCommitResources resources);
	ReclusterCommitInfo(shared_ptr<DataTable> storage, shared_ptr<const RowGroupLayout> old_layout,
	                    shared_ptr<RowGroupLayout> pending_layout, sort_run_id_t recovered_run_id,
	                    vector<block_id_t> recovered_blocks, RowGroupRange retired_range);
	~ReclusterCommitInfo();

	void WriteToWAL(WriteAheadLog &wal);
	void Commit(transaction_t commit_id, CommitDropState &drop_state);
	void RevertCommit();
	void FinalizeCommit();
	void Rollback();
	void Cleanup(transaction_t lowest_active_transaction);

private:
	void RevertLayout();
	void ReleaseRecoveredBlocks();
	void ReleaseRecoveredBlocksNoThrow() noexcept;
	void CommitRuntimeWALRetention();

private:
	enum class ReclusterCommitLifecycle : uint8_t { PREPARED, APPLIED, FINALIZED, ROLLED_BACK };

	shared_ptr<RangeTask> task;
	shared_ptr<TableReclusterState> table_state;
	shared_ptr<DataTable> storage;
	shared_ptr<const RowGroupLayout> old_layout;
	shared_ptr<RowGroupLayout> pending_layout;
	unique_ptr<PreparedReclusterWAL> prepared_wal;
	sort_run_id_t recovered_run_id = INVALID_SORT_RUN_ID;
	vector<block_id_t> recovered_blocks;
	idx_t recovered_owned_block_count = 0;
	PreparedReclusterRetirement retirement;
	ReclusterWALRetentionReservation wal_retention;
	uint64_t wal_checkpoint_iteration = 0;
	ReclusterCommitLifecycle state = ReclusterCommitLifecycle::PREPARED;
	bool layout_published = false;
	bool layout_version_advanced = false;
};

struct ReclusterUndoData {
	ReclusterCommitInfo *info;
};

} // namespace duckdb
