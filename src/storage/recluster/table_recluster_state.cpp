#include "duckdb/storage/recluster/table_recluster_state.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/time_point.hpp"

namespace duckdb {

TableReclusterState::TableReclusterState(uint64_t initialization_token_p)
    : initialization_token(initialization_token_p) {
	if (initialization_token == 0) {
		throw InternalException("A table recluster state requires a non-zero initialization token");
	}
}

bool TableReclusterState::AcceptsNewTasks() const {
	lock_guard<mutex> guard(task_lock);
	return accept_new_tasks;
}

void TableReclusterState::SetAcceptNewTasks(bool accept) {
	lock_guard<mutex> guard(task_lock);
	accept_new_tasks = accept;
}

void TableReclusterState::SynchronizeCatalog(persistent_table_id_t table_id_p, sort_order_id_t sort_order_id,
                                             uint64_t storage_generation_id, bool accept_new_tasks_p) {
	lock_guard<mutex> guard(task_lock);
	if (table_id_p == hugeint_t(0, 0)) {
		throw InternalException("A table recluster state requires a non-zero persistent table ID");
	}
	if (table_id != hugeint_t(0, 0) && table_id != table_id_p) {
		throw InternalException("A table recluster state cannot change its persistent table ID");
	}
	table_id = table_id_p;
	if (current_sort_order_id != sort_order_id || current_storage_generation_id != storage_generation_id) {
		last_checkpoint.reset();
	}
	current_sort_order_id = sort_order_id;
	current_storage_generation_id = storage_generation_id;
	accept_new_tasks = accept_new_tasks_p && sort_order_id != INVALID_SORT_ORDER_ID;
}

persistent_table_id_t TableReclusterState::GetTableId() const {
	lock_guard<mutex> guard(task_lock);
	return table_id;
}

sort_order_id_t TableReclusterState::GetCurrentSortOrderId() const {
	lock_guard<mutex> guard(task_lock);
	return current_sort_order_id;
}

uint64_t TableReclusterState::GetCurrentStorageGenerationId() const {
	lock_guard<mutex> guard(task_lock);
	return current_storage_generation_id;
}

bool TableReclusterState::TryInstallCheckpointSnapshot(sort_order_id_t sort_order_id, uint64_t storage_generation_id,
                                                       CheckpointLayoutSnapshot snapshot) noexcept {
	lock_guard<mutex> guard(task_lock);
	if (!accept_new_tasks || current_sort_order_id != sort_order_id ||
	    current_storage_generation_id != storage_generation_id ||
	    snapshot.storage_generation_id != storage_generation_id) {
		return false;
	}
	last_checkpoint = std::move(snapshot);
	return true;
}

bool TableReclusterState::HasUsableCheckpoint() const {
	lock_guard<mutex> guard(task_lock);
	return last_checkpoint && last_checkpoint->checkpoint_number > 0;
}

optional<CheckpointLayoutSnapshot> TableReclusterState::GetLastCheckpoint() const {
	lock_guard<mutex> guard(task_lock);
	return last_checkpoint;
}

void TableReclusterState::ClearLastCheckpoint() {
	lock_guard<mutex> guard(task_lock);
	last_checkpoint.reset();
}

bool TableReclusterState::RangeIsAvailable(const RowGroupRange &range) const {
	auto next = reserved_ranges.lower_bound(range.start);
	if (next != reserved_ranges.end() && next->second.range.Overlaps(range)) {
		return false;
	}
	if (next != reserved_ranges.begin()) {
		auto previous = std::prev(next);
		if (previous->second.range.Overlaps(range)) {
			return false;
		}
	}
	return true;
}

bool TableReclusterState::TryRegisterTask(shared_ptr<RangeTask> task) {
	if (!task) {
		throw InternalException("Cannot register a null recluster task");
	}
	auto task_id = task->GetTaskId();
	auto range = task->GetRange();

	lock_guard<mutex> guard(task_lock);
	if (!accept_new_tasks || tasks.find(task_id) != tasks.end() ||
	    reservation_starts.find(task_id) != reservation_starts.end() || !RangeIsAvailable(range)) {
		return false;
	}

	reservation_starts.reserve(reservation_starts.size() + 1);
	tasks.reserve(tasks.size() + 1);
	auto reservation = reserved_ranges.emplace(range.start, RangeReservation {range, task_id});
	if (!reservation.second) {
		return false;
	}
	try {
		reservation_starts.emplace(task_id, range.start);
		tasks.emplace(task_id, std::move(task));
	} catch (...) {
		reservation_starts.erase(task_id);
		tasks.erase(task_id);
		reserved_ranges.erase(reservation.first);
		throw;
	}
	return true;
}

bool TableReclusterState::OwnsTask(const shared_ptr<RangeTask> &task) const {
	if (!task) {
		return false;
	}
	lock_guard<mutex> guard(task_lock);
	auto task_entry = tasks.find(task->GetTaskId());
	auto reservation_entry = reservation_starts.find(task->GetTaskId());
	if (task_entry == tasks.end() || task_entry->second.get() != task.get() ||
	    reservation_entry == reservation_starts.end() || reservation_entry->second != task->GetRange().start) {
		return false;
	}
	auto range_entry = reserved_ranges.find(reservation_entry->second);
	return range_entry != reserved_ranges.end() && range_entry->second.task_id == task->GetTaskId() &&
	       range_entry->second.range.start == task->GetRange().start &&
	       range_entry->second.range.end == task->GetRange().end;
}

shared_ptr<RangeTask> TableReclusterState::GetTask(recluster_task_id_t task_id) const {
	lock_guard<mutex> guard(task_lock);
	auto entry = tasks.find(task_id);
	return entry == tasks.end() ? nullptr : entry->second;
}

shared_ptr<RangeTask> TableReclusterState::GetTaskForRow(row_t row_id) const {
	lock_guard<mutex> guard(task_lock);
	auto range_entry = reserved_ranges.upper_bound(row_id);
	if (range_entry == reserved_ranges.begin()) {
		return nullptr;
	}
	range_entry--;
	if (!range_entry->second.range.Contains(row_id)) {
		return nullptr;
	}
	auto task_entry = tasks.find(range_entry->second.task_id);
	if (task_entry == tasks.end()) {
		return nullptr;
	}
	return task_entry->second;
}

vector<shared_ptr<RangeTask>> TableReclusterState::DisableAndGetTasks() {
	lock_guard<mutex> guard(task_lock);
	vector<shared_ptr<RangeTask>> result;
	result.reserve(tasks.size());
	for (auto &entry : tasks) {
		result.push_back(entry.second);
	}
	accept_new_tasks = false;
	return result;
}

void TableReclusterState::RemoveTaskInternal(recluster_task_id_t task_id) {
	auto range_entry = reservation_starts.find(task_id);
	if (range_entry != reservation_starts.end()) {
		reserved_ranges.erase(range_entry->second);
		reservation_starts.erase(range_entry);
	}
	tasks.erase(task_id);
}

void TableReclusterState::RemoveTask(recluster_task_id_t task_id) {
	lock_guard<mutex> guard(task_lock);
	RemoveTaskInternal(task_id);
}

vector<RowGroupRange> TableReclusterState::GetReservedRanges() const {
	lock_guard<mutex> guard(task_lock);
	vector<RowGroupRange> result;
	result.reserve(reserved_ranges.size());
	for (auto &entry : reserved_ranges) {
		result.push_back(entry.second.range);
	}
	return result;
}

TableReclusterTaskStatus TableReclusterState::GetTaskStatus() const {
	lock_guard<mutex> guard(task_lock);
	TableReclusterTaskStatus result;
	for (auto &entry : tasks) {
		auto task_status = entry.second->GetStatusSnapshot();
		switch (task_status.state) {
		case RangeTaskState::STARTING:
		case RangeTaskState::PREPARING:
		case RangeTaskState::CATCHING_UP_DELETES:
			result.active_prepare_tasks++;
			break;
		case RangeTaskState::PREPARED:
		case RangeTaskState::FINALIZING:
		case RangeTaskState::COMMITTING:
			result.pending_finalize_tasks++;
			break;
		default:
			break;
		}
		result.pending_delete_rows =
		    SaturatingAddReclusterValue(result.pending_delete_rows, task_status.pending_delete_rows);
		result.prepared_bytes = SaturatingAddReclusterValue(result.prepared_bytes, task_status.prepared_bytes);
	}
	return result;
}

optional<int64_t> TableReclusterState::ObserveRemainingWorkAgeMs(bool has_remaining_work) {
	lock_guard<mutex> guard(task_lock);
	if (!has_remaining_work) {
		remaining_work_observed_ms.reset();
		return nullopt;
	}
	auto now_ms = TimePoint::GetTickMs();
	if (!remaining_work_observed_ms) {
		remaining_work_observed_ms = now_ms;
	}
	return now_ms - *remaining_work_observed_ms;
}

void TableReclusterState::SetLastError(string error) {
	lock_guard<mutex> guard(task_lock);
	last_error = std::move(error);
}

optional<string> TableReclusterState::GetLastError() const {
	lock_guard<mutex> guard(task_lock);
	return last_error;
}

idx_t TableReclusterState::GetTaskCount() const {
	lock_guard<mutex> guard(task_lock);
	return tasks.size();
}

} // namespace duckdb
