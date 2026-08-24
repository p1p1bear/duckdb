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
#include "duckdb/storage/recluster/recluster_retirement.hpp"
#include "duckdb/storage/recluster/recluster_wal_retention.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class AttachedDatabase;
class DataTable;
class DuckTableEntry;
struct ProducerToken;
class RangeTask;
class ReclusterAutoTask;
struct ReclusterAutoSchedulerState;
class TableReclusterState;

enum class ReclusterExplicitState : uint8_t { COMPLETE, BUDGET_EXHAUSTED, NO_ELIGIBLE_RANGE, ALREADY_RUNNING, FAILED };

struct ReclusterExplicitOptions {
	bool create_checkpoint = false;
	idx_t max_bytes = 0;
	idx_t max_tasks = 0;
};

struct ReclusterExplicitResult {
	string table_name;
	idx_t tasks_completed = 0;
	idx_t input_bytes = 0;
	idx_t output_bytes = 0;
	idx_t remaining_recluster_bytes = 0;
	ReclusterExplicitState state = ReclusterExplicitState::NO_ELIGIBLE_RANGE;
	string message;
};

const char *ReclusterExplicitStateToString(ReclusterExplicitState state);

enum class ReclusterTaskStartStatus : uint8_t { STARTED, STALE_CANDIDATE, RANGE_UNAVAILABLE, CANCELLED };
enum class ReclusterTaskFinalizeStatus : uint8_t { PUBLISHED, RETRY, STALE_TASK, CANCELLED };

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
	~ReclusterManager();

	uint64_t BeginCheckpoint();
	optional<PendingCheckpointTableState> PrepareCheckpoint(DuckTableEntry &table, uint64_t checkpoint_number);
	void OnCheckpointSuccess(vector<PendingCheckpointTableState> &&states) noexcept;

	//! Called after the checkpoint root is loaded and before WAL replay.
	void InitializeCheckpointTables();
	//! Called after WAL replay to synchronize the final recovered catalog without creating new candidates.
	void SynchronizeLoadedCatalog();
	ReclusterTaskStartResult TryStartTask(DuckTableEntry &table, const ReclusterCandidate &candidate,
	                                      optional_ptr<ClientContext> driver_context = nullptr);
	ReclusterTaskFinalizeStatus FinalizeTask(DuckTableEntry &table, const shared_ptr<RangeTask> &task);
	ReclusterExplicitResult RunExplicit(ClientContext &context, const QualifiedName &table_name,
	                                    const ReclusterExplicitOptions &options);
	//! Coalesces a commit/checkpoint wake-up into one background scheduler task.
	void RequestAutoRecluster() noexcept;
	//! Stops accepting automatic work and drains any queued/running task before database close.
	void StopAutoRecluster() noexcept;
	//! Drains this manager's background work. Used by shutdown and deterministic tests.
	void WaitForAutoRecluster();
	ReclusterWALBlockRetention &GetWALBlockRetention() {
		return wal_block_retention;
	}
	ReclusterRetirementRegistry &GetRetirementRegistry() {
		return retirement_registry;
	}

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
	friend class ReclusterAutoTask;

	void InitializeAutoScheduler();
	void RunAutoReclusterPass() noexcept;
	void FinishAutoReclusterTask() noexcept;
	void ScheduleAutoReclusterTask() noexcept;
	bool AutoReclusterEnabled() const noexcept;
	shared_ptr<TableReclusterState> SynchronizeTable(DuckTableEntry &table);
	uint64_t AllocateInitializationToken();

private:
	AttachedDatabase &db;
	ReclusterWALBlockRetention wal_block_retention;
	ReclusterRetirementRegistry retirement_registry;
	shared_ptr<ReclusterAutoSchedulerState> auto_scheduler_state;
	unique_ptr<ProducerToken> auto_scheduler_producer;
	mutex queue_lock;
	StorageLock layout_publish_lock;
	unordered_map<persistent_table_id_t, weak_ptr<TableReclusterState>> tables;
	atomic<uint64_t> next_checkpoint_number {1};
	atomic<uint64_t> next_initialization_token {1};
};

} // namespace duckdb
