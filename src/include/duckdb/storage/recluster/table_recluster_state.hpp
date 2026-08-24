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
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
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
	void SynchronizeCatalog(persistent_table_id_t table_id, sort_order_id_t sort_order_id,
	                        uint64_t storage_generation_id, bool accept_new_tasks);
	persistent_table_id_t GetTableId() const;
	sort_order_id_t GetCurrentSortOrderId() const;
	uint64_t GetCurrentStorageGenerationId() const;
	bool TryInstallCheckpointSnapshot(sort_order_id_t sort_order_id, uint64_t storage_generation_id,
	                                  CheckpointLayoutSnapshot snapshot) noexcept;
	optional<CheckpointLayoutSnapshot> GetLastCheckpoint() const;
	void ClearLastCheckpoint();
	bool TryRegisterTask(shared_ptr<RangeTask> task);
	bool OwnsTask(const shared_ptr<RangeTask> &task) const;
	shared_ptr<RangeTask> GetTask(recluster_task_id_t task_id) const;
	shared_ptr<RangeTask> GetTaskForRow(row_t row_id) const;
	vector<shared_ptr<RangeTask>> DisableAndGetTasks();
	void RemoveTask(recluster_task_id_t task_id);
	vector<RowGroupRange> GetReservedRanges() const;

	unique_lock<mutex> LockFinalize() {
		return unique_lock<mutex>(finalize_mutex);
	}
	unique_lock<mutex> TryLockExplicit() {
		return unique_lock<mutex>(explicit_mutex, std::try_to_lock);
	}
	idx_t GetTaskCount() const;

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
	mutable mutex explicit_mutex;
	mutable mutex task_lock;
	bool accept_new_tasks = false;
	persistent_table_id_t table_id = hugeint_t(0, 0);
	sort_order_id_t current_sort_order_id = INVALID_SORT_ORDER_ID;
	uint64_t current_storage_generation_id = 0;
	optional<CheckpointLayoutSnapshot> last_checkpoint;
	map<row_t, RangeReservation> reserved_ranges;
	unordered_map<recluster_task_id_t, row_t> reservation_starts;
	unordered_map<recluster_task_id_t, shared_ptr<RangeTask>> tasks;
};

} // namespace duckdb
