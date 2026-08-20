//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/range_task.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/recluster/row_group_layout.hpp"

#include <atomic>

namespace duckdb {

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

class RangeTask {
public:
	RangeTask(recluster_task_id_t task_id, RowGroupRange range);

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
};

} // namespace duckdb
