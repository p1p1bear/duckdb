//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_commit.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class CommitDropState;
class DataTable;
class RangeTask;
class RowGroupLayout;
class TableReclusterState;
class WriteAheadLog;

class ReclusterCommitInfo {
public:
	ReclusterCommitInfo(shared_ptr<RangeTask> task, shared_ptr<TableReclusterState> table_state,
	                    shared_ptr<DataTable> storage, shared_ptr<const RowGroupLayout> old_layout,
	                    shared_ptr<RowGroupLayout> pending_layout, vector<row_t> final_deleted_new_rowids,
	                    delete_sequence_t journal_resolved_through);
	~ReclusterCommitInfo();

	void WriteToWAL(WriteAheadLog &wal) const;
	void Commit(transaction_t commit_id, CommitDropState &drop_state);
	void RevertCommit();
	void FinalizeCommit();
	void Rollback();
	void Cleanup(transaction_t lowest_active_transaction);

private:
	void RevertLayout();

private:
	enum class ReclusterCommitLifecycle : uint8_t { PREPARED, APPLIED, FINALIZED, ROLLED_BACK };

	shared_ptr<RangeTask> task;
	shared_ptr<TableReclusterState> table_state;
	shared_ptr<DataTable> storage;
	shared_ptr<const RowGroupLayout> old_layout;
	shared_ptr<RowGroupLayout> pending_layout;
	shared_ptr<const RowGroupLayout> published_layout;
	vector<row_t> final_deleted_new_rowids;
	delete_sequence_t journal_resolved_through;
	ReclusterCommitLifecycle state = ReclusterCommitLifecycle::PREPARED;
	bool layout_published = false;
	bool layout_version_advanced = false;
};

struct ReclusterUndoData {
	ReclusterCommitInfo *info;
};

} // namespace duckdb
