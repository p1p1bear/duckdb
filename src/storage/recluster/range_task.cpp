#include "duckdb/storage/recluster/range_task.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"

namespace duckdb {

static constexpr uint16_t RANGE_TASK_STATE_MASK = 0x00ff;
static constexpr uint16_t RANGE_TASK_CANCEL_REQUESTED = 0x0100;
static constexpr uint16_t RANGE_TASK_PUBLISH_FORBIDDEN = 0x0200;
static constexpr uint16_t RANGE_TASK_FLAG_MASK = RANGE_TASK_CANCEL_REQUESTED | RANGE_TASK_PUBLISH_FORBIDDEN;

static RowGroupRange GetTaskContextRange(const unique_ptr<ReclusterTaskContext> &task_context) {
	if (!task_context) {
		throw InternalException("A recluster task requires a task context");
	}
	return task_context->GetCandidate().range;
}

RangeTask::RangeTask(recluster_task_id_t task_id_p, RowGroupRange range_p)
    : task_id(task_id_p), range(range_p), control(static_cast<uint16_t>(RangeTaskState::STARTING)) {
	if (task_id == recluster_task_id_t(0, 0)) {
		throw InternalException("A recluster task requires a non-zero task ID");
	}
	if (range.start < 0 || range.start >= range.end) {
		throw InternalException("A recluster task requires a valid row group range");
	}
}

RangeTask::RangeTask(recluster_task_id_t task_id_p, unique_ptr<ReclusterTaskContext> task_context_p)
    : task_id(task_id_p), range(GetTaskContextRange(task_context_p)),
      control(static_cast<uint16_t>(RangeTaskState::STARTING)), task_context(std::move(task_context_p)) {
	if (task_id == recluster_task_id_t(0, 0)) {
		throw InternalException("A recluster task requires a non-zero task ID");
	}
	if (range.start < 0 || range.start >= range.end) {
		throw InternalException("A recluster task requires a valid row group range");
	}
}

RangeTask::~RangeTask() {
}

ReclusterTaskContext &RangeTask::GetTaskContext() {
	if (!task_context) {
		throw InternalException("Recluster task has no execution context");
	}
	return *task_context;
}

const ReclusterTaskContext &RangeTask::GetTaskContext() const {
	if (!task_context) {
		throw InternalException("Recluster task has no execution context");
	}
	return *task_context;
}

RangeTaskState RangeTask::DecodeState(uint16_t control) {
	return static_cast<RangeTaskState>(control & RANGE_TASK_STATE_MASK);
}

RangeTaskState RangeTask::GetState() const {
	return DecodeState(control.load(std::memory_order_acquire));
}

bool RangeTask::IsCancelRequested() const {
	return (control.load(std::memory_order_acquire) & RANGE_TASK_CANCEL_REQUESTED) != 0;
}

bool RangeTask::IsPublishForbidden() const {
	return (control.load(std::memory_order_acquire) & RANGE_TASK_PUBLISH_FORBIDDEN) != 0;
}

bool RangeTask::IsFinished() const {
	auto state = GetState();
	return state == RangeTaskState::PUBLISHED || state == RangeTaskState::DETACHED || state == RangeTaskState::FAILED;
}

void RangeTask::RequestCancel() noexcept {
	control.fetch_or(RANGE_TASK_CANCEL_REQUESTED, std::memory_order_acq_rel);
}

void RangeTask::DisablePublishForJournalFailure() noexcept {
	control.fetch_or(RANGE_TASK_PUBLISH_FORBIDDEN, std::memory_order_acq_rel);
}

bool RangeTask::IsProgressTransition(RangeTaskState expected, RangeTaskState target) {
	switch (expected) {
	case RangeTaskState::STARTING:
		return target == RangeTaskState::PREPARING;
	case RangeTaskState::PREPARING:
		return target == RangeTaskState::CATCHING_UP_DELETES;
	case RangeTaskState::CATCHING_UP_DELETES:
		return target == RangeTaskState::PREPARED;
	case RangeTaskState::PREPARED:
		return target == RangeTaskState::FINALIZING;
	case RangeTaskState::FINALIZING:
		return target == RangeTaskState::CATCHING_UP_DELETES;
	default:
		return false;
	}
}

bool RangeTask::TryReplaceState(uint16_t &expected_control, RangeTaskState target) noexcept {
	auto desired = static_cast<uint16_t>((expected_control & RANGE_TASK_FLAG_MASK) | static_cast<uint16_t>(target));
	return control.compare_exchange_strong(expected_control, desired, std::memory_order_acq_rel,
	                                       std::memory_order_acquire);
}

bool RangeTask::TryAdvance(RangeTaskState expected, RangeTaskState target) noexcept {
	if (!IsProgressTransition(expected, target)) {
		return false;
	}
	auto current = control.load(std::memory_order_acquire);
	while (DecodeState(current) == expected && (current & RANGE_TASK_FLAG_MASK) == 0) {
		if (TryReplaceState(current, target)) {
			return true;
		}
	}
	return false;
}

bool RangeTask::TryEnterCancelling() noexcept {
	auto current = control.load(std::memory_order_acquire);
	while ((current & RANGE_TASK_FLAG_MASK) != 0) {
		auto state = DecodeState(current);
		if (state == RangeTaskState::COMMITTING || state == RangeTaskState::PUBLISHED ||
		    state == RangeTaskState::CANCELLING || state == RangeTaskState::DETACHED ||
		    state == RangeTaskState::FAILED) {
			return false;
		}
		if (TryReplaceState(current, RangeTaskState::CANCELLING)) {
			return true;
		}
	}
	return false;
}

bool RangeTask::TryEnterCommitting() noexcept {
	auto current = control.load(std::memory_order_acquire);
	while (DecodeState(current) == RangeTaskState::FINALIZING && (current & RANGE_TASK_FLAG_MASK) == 0) {
		if (TryReplaceState(current, RangeTaskState::COMMITTING)) {
			return true;
		}
	}
	return false;
}

bool RangeTask::TryFinishCommit(bool success) noexcept {
	auto current = control.load(std::memory_order_acquire);
	while (DecodeState(current) == RangeTaskState::COMMITTING) {
		if (TryReplaceState(current, success ? RangeTaskState::PUBLISHED : RangeTaskState::FAILED)) {
			return true;
		}
	}
	return false;
}

bool RangeTask::TryDetach() noexcept {
	auto current = control.load(std::memory_order_acquire);
	while (DecodeState(current) == RangeTaskState::CANCELLING) {
		if (TryReplaceState(current, RangeTaskState::DETACHED)) {
			return true;
		}
	}
	return false;
}

bool RangeTask::TryFail() noexcept {
	auto current = control.load(std::memory_order_acquire);
	while (true) {
		auto state = DecodeState(current);
		if (state == RangeTaskState::PUBLISHED || state == RangeTaskState::DETACHED ||
		    state == RangeTaskState::FAILED) {
			return false;
		}
		if (TryReplaceState(current, RangeTaskState::FAILED)) {
			return true;
		}
	}
}

} // namespace duckdb
