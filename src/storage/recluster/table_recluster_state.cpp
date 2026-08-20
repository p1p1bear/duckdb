#include "duckdb/storage/recluster/table_recluster_state.hpp"

#include "duckdb/common/exception.hpp"

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

shared_ptr<RangeTask> TableReclusterState::GetTask(recluster_task_id_t task_id) const {
	lock_guard<mutex> guard(task_lock);
	auto entry = tasks.find(task_id);
	return entry == tasks.end() ? nullptr : entry->second;
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

} // namespace duckdb
