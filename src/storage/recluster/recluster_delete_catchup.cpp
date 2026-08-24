#include "duckdb/storage/recluster/recluster_delete_catchup.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/row_id_remap_store.hpp"

namespace duckdb {

ReclusterDeleteCatchup::ReclusterDeleteCatchup(RangeTask &task_p) : task(task_p) {
	CheckTask();
	if (!task.HasTaskContext() || !task.GetTaskContext().HasOutput()) {
		throw InternalException("Recluster DELETE catch-up requires private output");
	}
}

void ReclusterDeleteCatchup::CheckTask() const {
	if (task.IsCancelRequested() || task.IsPublishForbidden()) {
		throw InterruptException("Recluster task was cancelled during DELETE catch-up");
	}
	if (task.GetState() != RangeTaskState::CATCHING_UP_DELETES) {
		throw InternalException("Recluster DELETE catch-up observed an invalid task state");
	}
	task.GetTaskContext().InterruptCheck();
}

ReclusterDeleteCatchupResult ReclusterDeleteCatchup::Run(idx_t max_slots, idx_t max_rowids) {
	CheckTask();
	auto &limits = task.GetDeleteJournalLimits();
	max_slots = max_slots == 0 ? limits.max_slots : max_slots;
	max_rowids = max_rowids == 0 ? limits.max_rowids : max_rowids;
	if (max_slots == 0 || max_rowids == 0) {
		throw InternalException("Recluster DELETE catch-up requires non-zero scan limits");
	}

	auto &task_context = task.GetTaskContext();
	auto &output = task_context.GetOutput();
	auto previous_sequence = output.GetManifest().header.last_applied_delete_sequence;
	auto scan = task.ScanResolvedDeletes(previous_sequence, max_slots, max_rowids);

	vector<row_t> new_rowids;
	new_rowids.reserve(scan.committed_rowid_count);
	for (auto &slot_ref : scan.slots) {
		auto &slot = slot_ref.get();
		if (slot.GetState() != DeleteSlotState::COMMITTED) {
			continue;
		}
		for (auto old_rowid : slot.GetOldRowIds()) {
			auto new_rowid = task_context.GetRowIdRemap().GetNewRowId(old_rowid);
			if (new_rowid != INVALID_REMAP_ROW_ID) {
				new_rowids.push_back(new_rowid);
			}
		}
	}

	ReclusterDeleteCatchupResult result;
	result.applied_through = scan.resolved_through;
	result.resolved_slot_count = scan.slots.size();
	result.mapped_rowid_count = new_rowids.size();
	result.blocked_by_reserved = scan.blocked_by_reserved;
	result.limit_exceeded = scan.limit_exceeded;
	if (scan.resolved_through > previous_sequence) {
		result.deleted_row_count = output.ApplyDeleteCatchup(std::move(new_rowids), scan.resolved_through);
	}

	CheckTask();
	if (!scan.limit_exceeded && !task.TryAdvance(RangeTaskState::CATCHING_UP_DELETES, RangeTaskState::PREPARED)) {
		CheckTask();
		throw InternalException("Failed to finish recluster DELETE catch-up");
	}
	return result;
}

} // namespace duckdb
