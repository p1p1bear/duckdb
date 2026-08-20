#include "duckdb/storage/recluster/recluster_commit.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/wal_entry.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/commit_state.hpp"

namespace duckdb {

ReclusterCommitInfo::ReclusterCommitInfo(shared_ptr<RangeTask> task_p, shared_ptr<TableReclusterState> table_state_p,
                                         shared_ptr<DataTable> storage_p, shared_ptr<const RowGroupLayout> old_layout_p,
                                         shared_ptr<RowGroupLayout> pending_layout_p,
                                         vector<row_t> final_deleted_new_rowids_p,
                                         delete_sequence_t journal_resolved_through_p)
    : task(std::move(task_p)), table_state(std::move(table_state_p)), storage(std::move(storage_p)),
      old_layout(std::move(old_layout_p)), pending_layout(std::move(pending_layout_p)),
      final_deleted_new_rowids(std::move(final_deleted_new_rowids_p)),
      journal_resolved_through(journal_resolved_through_p) {
	if (!task || !table_state || !storage || !old_layout || !pending_layout || !task->HasTaskContext() ||
	    !task->GetTaskContext().HasOutput() || task->GetTaskContext().GetStorage().get() != storage.get() ||
	    pending_layout->visible_from != 0 || old_layout->layout_version == NumericLimits<layout_version_t>::Maximum() ||
	    pending_layout->layout_version != old_layout->layout_version + 1 ||
	    pending_layout->base_tree.get() != old_layout->base_tree.get() ||
	    journal_resolved_through <
	        task->GetTaskContext().GetOutput().GetManifest().header.last_applied_delete_sequence) {
		throw InternalException("Invalid recluster commit state");
	}
	for (idx_t row_index = 0; row_index < final_deleted_new_rowids.size(); row_index++) {
		auto row_id = final_deleted_new_rowids[row_index];
		if (!task->GetRange().Contains(row_id) ||
		    (row_index > 0 && final_deleted_new_rowids[row_index - 1] >= row_id)) {
			throw InternalException("Invalid final recluster DELETE row IDs");
		}
	}
}

ReclusterCommitInfo::~ReclusterCommitInfo() {
}

void ReclusterCommitInfo::WriteToWAL(WriteAheadLog &wal) const {
	if (state != ReclusterCommitLifecycle::PREPARED) {
		throw InternalException("Cannot write an applied recluster commit to the WAL");
	}
	auto &manifest = task->GetTaskContext().GetOutput().GetManifest();
	auto delete_chunk_count = final_deleted_new_rowids.empty()
	                              ? 0
	                              : NumericCast<uint32_t>((final_deleted_new_rowids.size() + STANDARD_VECTOR_SIZE - 1) /
	                                                      STANDARD_VECTOR_SIZE);
	WALReclusterEntry header;
	header.table_id = manifest.header.table_id;
	header.task_id = manifest.header.task_id;
	header.expected_layout_version = old_layout->layout_version;
	header.target_layout_version = pending_layout->layout_version;
	header.range_start = manifest.header.input_range.start;
	header.range_end = manifest.header.input_range.end;
	header.manifest_pointer = task->GetTaskContext().GetOutput().GetManifestPointer();
	header.manifest_size = manifest.payload_size;
	header.manifest_checksum = manifest.checksum;
	header.journal_resolved_through = journal_resolved_through;
	header.final_delete_row_count = final_deleted_new_rowids.size();
	header.delete_chunk_count = delete_chunk_count;
	wal.WriteRecluster(header);

	for (uint32_t chunk_index = 0; chunk_index < delete_chunk_count; chunk_index++) {
		auto begin = NumericCast<idx_t>(chunk_index) * STANDARD_VECTOR_SIZE;
		auto end = MinValue<idx_t>(begin + STANDARD_VECTOR_SIZE, final_deleted_new_rowids.size());
		WALReclusterDeleteEntry delete_entry;
		delete_entry.table_id = header.table_id;
		delete_entry.task_id = header.task_id;
		delete_entry.chunk_index = chunk_index;
		delete_entry.new_rowids.assign(final_deleted_new_rowids.begin() + NumericCast<int64_t>(begin),
		                               final_deleted_new_rowids.begin() + NumericCast<int64_t>(end));
		wal.WriteReclusterDelete(delete_entry);
	}
}

void ReclusterCommitInfo::Commit(transaction_t commit_id, CommitDropState &drop_state) {
	if (state != ReclusterCommitLifecycle::PREPARED || task->GetState() != RangeTaskState::COMMITTING ||
	    layout_published) {
		throw InternalException("Invalid recluster commit transition");
	}

	try {
		auto &output = task->GetTaskContext().GetOutput();
		output.ApplyFinalDeletes(final_deleted_new_rowids);
		pending_layout->visible_from = commit_id;
		published_layout = pending_layout;
		storage->GetRowGroupCollection()->PublishLayout(published_layout);
		layout_published = true;

		auto &layout_version = storage->GetDataTableInfo()->GetSortStorage().current_layout_version;
		auto expected_version = old_layout->layout_version;
		if (!layout_version.compare_exchange_strong(expected_version, pending_layout->layout_version)) {
			throw InternalException("Recluster storage layout version changed during commit");
		}
		layout_version_advanced = true;
		state = ReclusterCommitLifecycle::APPLIED;
		drop_state.AddRecluster(*this);
	} catch (...) {
		if (layout_published) {
			RevertLayout();
		}
		pending_layout->visible_from = 0;
		published_layout.reset();
		state = ReclusterCommitLifecycle::PREPARED;
		throw;
	}
}

void ReclusterCommitInfo::RevertLayout() {
	if (!layout_published || !published_layout) {
		return;
	}
	storage->GetRowGroupCollection()->RevertPublishedLayout(published_layout, old_layout);
	if (layout_version_advanced) {
		auto &layout_version = storage->GetDataTableInfo()->GetSortStorage().current_layout_version;
		auto expected_version = pending_layout->layout_version;
		if (!layout_version.compare_exchange_strong(expected_version, old_layout->layout_version)) {
			throw InternalException("Recluster storage layout version changed during revert");
		}
		layout_version_advanced = false;
	}
	layout_published = false;
}

void ReclusterCommitInfo::RevertCommit() {
	if (state != ReclusterCommitLifecycle::APPLIED) {
		throw InternalException("Cannot revert an unapplied recluster commit");
	}
	RevertLayout();
	pending_layout->visible_from = 0;
	published_layout.reset();
	state = ReclusterCommitLifecycle::PREPARED;
}

void ReclusterCommitInfo::FinalizeCommit() {
	if (state != ReclusterCommitLifecycle::APPLIED || !layout_published) {
		throw InternalException("Cannot finalize an unapplied recluster commit");
	}
	task->GetTaskContext().GetOutput().MarkPublished();
	auto finished = task->TryFinishCommit(true);
	D_ASSERT(finished);
	(void)finished;
	table_state->RemoveTask(task->GetTaskId());
	state = ReclusterCommitLifecycle::FINALIZED;
}

void ReclusterCommitInfo::Rollback() {
	if (state == ReclusterCommitLifecycle::FINALIZED || state == ReclusterCommitLifecycle::ROLLED_BACK) {
		throw InternalException("Invalid recluster rollback transition");
	}
	if (layout_published) {
		RevertLayout();
	}
	task->GetTaskContext().GetOutput().Abort();
	if (task->GetState() == RangeTaskState::COMMITTING) {
		auto finished = task->TryFinishCommit(false);
		D_ASSERT(finished);
		(void)finished;
	} else {
		task->TryFail();
	}
	table_state->RemoveTask(task->GetTaskId());
	state = ReclusterCommitLifecycle::ROLLED_BACK;
}

void ReclusterCommitInfo::Cleanup(transaction_t lowest_active_transaction) {
	if (state != ReclusterCommitLifecycle::FINALIZED) {
		throw InternalException("Cannot clean up an unfinished recluster commit");
	}
	storage->GetRowGroupCollection()->CleanupLayoutHistory(lowest_active_transaction);
}

} // namespace duckdb
