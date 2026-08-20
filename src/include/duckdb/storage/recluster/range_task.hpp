//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/range_task.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
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

class RangeTask {
public:
	RangeTask(recluster_task_id_t task_id, RowGroupRange range);
	RangeTask(recluster_task_id_t task_id, unique_ptr<ReclusterTaskContext> task_context);
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
	unique_ptr<ReclusterTaskContext> task_context;
};

} // namespace duckdb
