#include "duckdb/storage/recluster/recluster_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <chrono>
#include <condition_variable>

namespace duckdb {

static constexpr idx_t AUTO_RECLUSTER_MAX_BYTES = 1ULL << 30;
static constexpr int64_t AUTO_RECLUSTER_CHECKPOINT_MIN_INTERVAL_MS = 60 * 1000;

struct ReclusterAutoSchedulerState {
	explicit ReclusterAutoSchedulerState(ReclusterManager &manager_p) : manager(manager_p) {
	}

	mutex lock;
	std::condition_variable cv;
	ReclusterManager &manager;
	bool closing = false;
	bool active = false;
	bool rerun_requested = false;
	bool checkpoint_requested = false;
	bool auto_checkpoint_completed = false;
	int64_t last_auto_checkpoint_ms = 0;
};

class ReclusterAutoTask final : public Task {
public:
	explicit ReclusterAutoTask(shared_ptr<ReclusterAutoSchedulerState> state_p) : state(std::move(state_p)) {
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		(void)mode;
		state->manager.RunAutoReclusterPass();
		state->manager.FinishAutoReclusterTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "ReclusterAutoTask";
	}

private:
	shared_ptr<ReclusterAutoSchedulerState> state;
};

static void LogAutoReclusterError(AttachedDatabase &db, const string &message) noexcept {
	try {
		DUCKDB_LOG_ERROR(db.GetDatabase(), "Automatic recluster: " + message);
	} catch (...) { // NOLINT: background maintenance cannot report a logging failure
	}
}

static void InvalidateAfterAutoReclusterError(AttachedDatabase &db, const std::exception &ex) noexcept {
	try {
		ErrorData error(ex);
		ValidChecker::Invalidate(db, "Automatic recluster failed with an internal error: " + error.Message());
	} catch (...) { // NOLINT: invalidation is already the terminal error path
	}
}

ReclusterManager::~ReclusterManager() {
	StopAutoRecluster();
	auto_scheduler_producer.reset();
	auto_scheduler_state.reset();
}

void ReclusterManager::StopAutoRecluster() noexcept {
	{
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		auto_scheduler_state->closing = true;
		auto_scheduler_state->rerun_requested = false;
		auto_scheduler_state->checkpoint_requested = false;
	}
	try {
		WaitForAutoRecluster();
	} catch (...) { // NOLINT: database close cannot report a background drain failure
	}
}

void ReclusterManager::InitializeAutoScheduler() {
	auto_scheduler_state = make_shared_ptr<ReclusterAutoSchedulerState>(*this);
	auto_scheduler_producer = TaskScheduler::GetScheduler(db.GetDatabase()).CreateProducer();
}

bool ReclusterManager::AutoReclusterEnabled() const noexcept {
	try {
		return !db.IsReadOnly() && !ValidChecker::IsInvalidated(db) &&
		       Settings::Get<AutoReclusterSetting>(db.GetDatabase());
	} catch (...) {
		return false;
	}
}

bool ReclusterManager::AutoCheckpointEnabled() const noexcept {
	try {
		return AutoReclusterEnabled() && Settings::Get<ReclusterTriggerCheckpointSetting>(db.GetDatabase());
	} catch (...) {
		return false;
	}
}

void ReclusterManager::ClearAutoCheckpointRequest() noexcept {
	lock_guard<mutex> guard(auto_scheduler_state->lock);
	auto_scheduler_state->checkpoint_requested = false;
}

void ReclusterManager::RequestAutoCheckpoint() noexcept {
	if (!AutoCheckpointEnabled()) {
		return;
	}
	auto now_ms = TimePoint::GetTickMs();
	{
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		if (auto_scheduler_state->closing) {
			return;
		}
		auto_scheduler_state->checkpoint_requested = true;
		if (auto_scheduler_state->auto_checkpoint_completed &&
		    now_ms - auto_scheduler_state->last_auto_checkpoint_ms < AUTO_RECLUSTER_CHECKPOINT_MIN_INTERVAL_MS) {
			return;
		}
	}

	try {
		Connection connection(db.GetDatabase());
		{
			lock_guard<mutex> guard(auto_scheduler_state->lock);
			if (auto_scheduler_state->closing || !auto_scheduler_state->checkpoint_requested) {
				return;
			}
		}
		if (db.GetStorageManager().GetWALSize() == 0) {
			return;
		}
		connection.context->RunFunctionInTransaction(
		    [&]() { TransactionManager::Get(db).Checkpoint(*connection.context, false); });
		now_ms = TimePoint::GetTickMs();
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		auto_scheduler_state->checkpoint_requested = false;
		auto_scheduler_state->auto_checkpoint_completed = true;
		auto_scheduler_state->last_auto_checkpoint_ms = now_ms;
	} catch (TransactionException &) {
		// A writer or checkpoint owns the lock. A later commit/checkpoint wake-up retries the pending request.
	} catch (FatalException &ex) {
		InvalidateAfterAutoReclusterError(db, ex);
	} catch (DataCorruptionException &ex) {
		InvalidateAfterAutoReclusterError(db, ex);
	} catch (SerializationException &ex) {
		InvalidateAfterAutoReclusterError(db, ex);
	} catch (InternalException &ex) {
		InvalidateAfterAutoReclusterError(db, ex);
	} catch (std::exception &ex) {
		LogAutoReclusterError(db, string("checkpoint request: ") + ex.what());
	} catch (...) {
		LogAutoReclusterError(db, "unknown checkpoint request error");
	}
}

void ReclusterManager::ScheduleAutoReclusterTask() noexcept {
	try {
		auto task = make_shared_ptr<ReclusterAutoTask>(auto_scheduler_state);
		TaskScheduler::GetScheduler(db.GetDatabase()).ScheduleTask(*auto_scheduler_producer, std::move(task));
	} catch (std::exception &ex) {
		LogAutoReclusterError(db, ex.what());
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		auto_scheduler_state->active = false;
		auto_scheduler_state->rerun_requested = false;
		auto_scheduler_state->cv.notify_all();
	} catch (...) {
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		auto_scheduler_state->active = false;
		auto_scheduler_state->rerun_requested = false;
		auto_scheduler_state->cv.notify_all();
	}
}

void ReclusterManager::RequestAutoRecluster() noexcept {
	if (!AutoReclusterEnabled()) {
		return;
	}
	auto allow_catalog_discovery = AutoCheckpointEnabled();
	{
		lock_guard<mutex> guard(queue_lock);
		if (tables.empty() && !allow_catalog_discovery) {
			return;
		}
	}
	{
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		if (auto_scheduler_state->closing) {
			return;
		}
		if (auto_scheduler_state->active) {
			auto_scheduler_state->rerun_requested = true;
			return;
		}
		auto_scheduler_state->active = true;
	}
	ScheduleAutoReclusterTask();
}

void ReclusterManager::FinishAutoReclusterTask() noexcept {
	bool schedule_again = false;
	{
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		if (!auto_scheduler_state->closing && auto_scheduler_state->rerun_requested && AutoReclusterEnabled()) {
			auto_scheduler_state->rerun_requested = false;
			schedule_again = true;
		} else {
			auto_scheduler_state->active = false;
			auto_scheduler_state->rerun_requested = false;
			auto_scheduler_state->cv.notify_all();
		}
	}
	if (schedule_again) {
		ScheduleAutoReclusterTask();
	}
}

void ReclusterManager::WaitForAutoRecluster() {
	auto &scheduler = TaskScheduler::GetScheduler(db.GetDatabase());
	while (true) {
		shared_ptr<Task> task;
		if (scheduler.GetTaskFromProducer(*auto_scheduler_producer, task)) {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
			continue;
		}
		unique_lock<mutex> guard(auto_scheduler_state->lock);
		if (!auto_scheduler_state->active) {
			return;
		}
		auto_scheduler_state->cv.wait_for(guard, std::chrono::milliseconds(10));
	}
}

void ReclusterManager::RunAutoReclusterPass() noexcept {
	{
		lock_guard<mutex> guard(auto_scheduler_state->lock);
		if (auto_scheduler_state->closing) {
			return;
		}
	}
	if (!AutoReclusterEnabled()) {
		return;
	}

	vector<QualifiedName> table_names;
	try {
		auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
		catalog.ScanSchemas([&](SchemaCatalogEntry &schema) {
			schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (!entry.internal && entry.type == CatalogType::TABLE_ENTRY) {
					auto &table = entry.Cast<DuckTableEntry>();
					if (table.SortEnabled()) {
						table_names.push_back(schema.GetQualifiedName(table.name));
					}
				}
			});
		});
	} catch (std::exception &ex) {
		LogAutoReclusterError(db, ex.what());
		return;
	} catch (...) {
		LogAutoReclusterError(db, "unknown error while scanning sorted tables");
		return;
	}

	bool checkpoint_needed = false;
	for (auto &table_name : table_names) {
		if (!AutoReclusterEnabled()) {
			return;
		}
		try {
			Connection connection(db.GetDatabase());
			ReclusterExplicitResult result;
			shared_ptr<TableReclusterState> table_state;
			connection.context->RunFunctionInTransaction([&]() {
				auto &table = Catalog::GetEntry<DuckTableEntry>(*connection.context, table_name);
				auto state = table.GetStorage().GetDataTableInfo()->GetReclusterState();
				if (!state) {
					state = SynchronizeTable(table);
				}
				if (!state) {
					return;
				}
				table_state = state;
				auto checkpoint = state->GetLastCheckpoint();
				if (!checkpoint || checkpoint->checkpoint_number == 0) {
					checkpoint_needed |= EstimateRemainingReclusterBytes(table.GetStorage(), *state) > 0;
					return;
				}
				ReclusterExplicitOptions options;
				options.max_bytes = AUTO_RECLUSTER_MAX_BYTES;
				options.max_tasks = 1;
				result = RunExplicit(*connection.context, table_name, options);
			});
			if (result.state == ReclusterExplicitState::FAILED) {
				if (table_state) {
					table_state->SetLastError(result.message);
				}
				LogAutoReclusterError(db, table_name.ToString() + ": " + result.message);
			} else if (result.state == ReclusterExplicitState::NO_ELIGIBLE_RANGE &&
			           result.remaining_recluster_bytes > 0) {
				checkpoint_needed = true;
			}
		} catch (FatalException &ex) {
			InvalidateAfterAutoReclusterError(db, ex);
			return;
		} catch (DataCorruptionException &ex) {
			InvalidateAfterAutoReclusterError(db, ex);
			return;
		} catch (SerializationException &ex) {
			InvalidateAfterAutoReclusterError(db, ex);
			return;
		} catch (InternalException &ex) {
			InvalidateAfterAutoReclusterError(db, ex);
			return;
		} catch (std::exception &ex) {
			LogAutoReclusterError(db, table_name.ToString() + ": " + ex.what());
		} catch (...) {
			LogAutoReclusterError(db, table_name.ToString() + ": unknown background error");
		}
	}
	if (checkpoint_needed) {
		RequestAutoCheckpoint();
	}
}

} // namespace duckdb
