//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/table_recluster_state.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/range_task.hpp"

namespace duckdb {

class TableReclusterState {
public:
	explicit TableReclusterState(uint64_t initialization_token);

	uint64_t GetInitializationToken() const {
		return initialization_token;
	}

	bool AcceptsNewTasks() const;
	void SetAcceptNewTasks(bool accept);
	bool TryRegisterTask(shared_ptr<RangeTask> task);
	shared_ptr<RangeTask> GetTask(recluster_task_id_t task_id) const;
	vector<shared_ptr<RangeTask>> DisableAndGetTasks();
	void RemoveTask(recluster_task_id_t task_id);
	vector<RowGroupRange> GetReservedRanges() const;

	unique_lock<mutex> LockFinalize() {
		return unique_lock<mutex>(finalize_mutex);
	}

private:
	struct RangeReservation {
		RowGroupRange range;
		recluster_task_id_t task_id;
	};

	bool RangeIsAvailable(const RowGroupRange &range) const;
	void RemoveTaskInternal(recluster_task_id_t task_id);

private:
	uint64_t initialization_token;
	mutable mutex finalize_mutex;
	mutable mutex task_lock;
	bool accept_new_tasks = false;
	map<row_t, RangeReservation> reserved_ranges;
	unordered_map<recluster_task_id_t, row_t> reservation_starts;
	unordered_map<recluster_task_id_t, shared_ptr<RangeTask>> tasks;
};

} // namespace duckdb
