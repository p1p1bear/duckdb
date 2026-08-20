//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/storage_lock.hpp"

namespace duckdb {

class AttachedDatabase;
class DataTable;
class DuckTableEntry;
class RangeTask;
class TableReclusterState;

enum class ReclusterTaskStartStatus : uint8_t { STARTED, STALE_CANDIDATE, RANGE_UNAVAILABLE, CANCELLED };

struct ReclusterTaskStartResult {
	ReclusterTaskStartStatus status = ReclusterTaskStartStatus::STALE_CANDIDATE;
	shared_ptr<RangeTask> task;
};

struct PendingCheckpointTableState {
	persistent_table_id_t table_id = hugeint_t(0, 0);
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	uint64_t initialization_token = 0;
	shared_ptr<TableReclusterState> state;
	shared_ptr<DataTable> storage;
	CheckpointLayoutSnapshot candidate_snapshot;
};

class ReclusterManager {
public:
	explicit ReclusterManager(AttachedDatabase &db);

	uint64_t BeginCheckpoint();
	optional<PendingCheckpointTableState> PrepareCheckpoint(DuckTableEntry &table, uint64_t checkpoint_number);
	void OnCheckpointSuccess(vector<PendingCheckpointTableState> &&states) noexcept;

	//! Called after the checkpoint root is loaded and before WAL replay.
	void InitializeCheckpointTables();
	//! Called after WAL replay to synchronize the final recovered catalog without creating new candidates.
	void SynchronizeLoadedCatalog();
	ReclusterTaskStartResult TryStartTask(DuckTableEntry &table, const ReclusterCandidate &candidate);

	unique_ptr<StorageLockKey> GetSharedLayoutPublishLock() {
		return layout_publish_lock.GetSharedLock();
	}
	unique_ptr<StorageLockKey> GetExclusiveLayoutPublishLock() {
		return layout_publish_lock.GetExclusiveLock();
	}
	unique_ptr<StorageLockKey> TryGetSharedLayoutPublishLock() {
		return layout_publish_lock.TryGetSharedLock();
	}

private:
	shared_ptr<TableReclusterState> SynchronizeTable(DuckTableEntry &table);
	uint64_t AllocateInitializationToken();

private:
	AttachedDatabase &db;
	mutex queue_lock;
	StorageLock layout_publish_lock;
	unordered_map<persistent_table_id_t, weak_ptr<TableReclusterState>> tables;
	atomic<uint64_t> next_checkpoint_number {1};
	atomic<uint64_t> next_initialization_token {1};
};

} // namespace duckdb
