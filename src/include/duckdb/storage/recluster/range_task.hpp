//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/range_task.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/recluster/recluster_delete_journal.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

#include <atomic>

namespace duckdb {

class ReclusterTaskContext;

enum class RangeTaskState : uint8_t {
	STARTING,
	PREPARING,
	CATCHING_UP_DELETES,
	PREPARED,
	FINALIZING,
	COMMITTING,
	PUBLISHED,
	CANCELLING,
	DETACHED,
	FAILED
};

struct RangeTaskStatusSnapshot {
	RangeTaskState state = RangeTaskState::STARTING;
	idx_t pending_delete_rows = 0;
	idx_t prepared_bytes = 0;
};

class RangeTask {
public:
	RangeTask(recluster_task_id_t task_id, RowGroupRange range,
	          ReclusterDeleteJournalLimits delete_journal_limits = {});
	RangeTask(recluster_task_id_t task_id, unique_ptr<ReclusterTaskContext> task_context,
	          ReclusterDeleteJournalLimits delete_journal_limits = {});
	~RangeTask();

	recluster_task_id_t GetTaskId() const {
		return task_id;
	}
	const RowGroupRange &GetRange() const {
		return range;
	}

	RangeTaskState GetState() const;
	bool IsCancelRequested() const;
	bool IsPublishForbidden() const;
	bool IsFinished() const;
	bool HasTaskContext() const {
		return task_context != nullptr;
	}
	ReclusterTaskContext &GetTaskContext();
	const ReclusterTaskContext &GetTaskContext() const;
	optional_ptr<ReclusterDeleteSlot> TryReserveDeleteSlot(vector<row_t> old_rowids) noexcept;
	bool ResolveDeleteSlot(ReclusterDeleteSlot &slot, DeleteSlotState target) noexcept;
	ReclusterDeleteJournalScan ScanResolvedDeletes(delete_sequence_t after_sequence, idx_t max_slots,
	                                               idx_t max_rowids) const;
	delete_sequence_t GetLatestDeleteSequence() const;
	RangeTaskStatusSnapshot GetStatusSnapshot() const;
	void UpdatePreparedOutputStatus(delete_sequence_t applied_delete_sequence, idx_t prepared_bytes) noexcept;
	const ReclusterDeleteJournalLimits &GetDeleteJournalLimits() const {
		return delete_journal.GetLimits();
	}

	void RequestCancel() noexcept;
	void DisablePublishForJournalFailure() noexcept;

	bool TryAdvance(RangeTaskState expected, RangeTaskState target) noexcept;
	bool TryEnterCancelling() noexcept;
	bool TryEnterCommitting() noexcept;
	bool TryFinishCommit(bool success) noexcept;
	bool TryDetach() noexcept;
	bool TryFail() noexcept;

private:
	static bool IsProgressTransition(RangeTaskState expected, RangeTaskState target);
	static RangeTaskState DecodeState(uint16_t control);
	bool TryReplaceState(uint16_t &expected_control, RangeTaskState target) noexcept;

private:
	recluster_task_id_t task_id;
	RowGroupRange range;
	std::atomic<uint16_t> control;
	ReclusterDeleteJournal delete_journal;
	unique_ptr<ReclusterTaskContext> task_context;
	std::atomic<delete_sequence_t> applied_delete_sequence {0};
	std::atomic<idx_t> prepared_bytes {0};
};

} // namespace duckdb
