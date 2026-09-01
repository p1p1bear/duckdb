#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <future>

using namespace duckdb; // NOLINT

static ReclusterCandidate SelectStartCandidate(Connection &con, const string &table_name,
                                               const ReclusterCandidateLimits &limits) {
	optional<ReclusterCandidate> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		ReclusterLayoutAnalysis analysis(*entry.GetStorage().GetRowGroupCollection(), entry.GetStorage().Columns(),
		                                 *state);
		auto selection = analysis.SelectCandidate(limits);
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		result = std::move(*selection.candidate);
	});
	REQUIRE(result);
	return std::move(*result);
}

static ReclusterTaskStartResult StartCandidate(Connection &con, const string &table_name,
                                               const ReclusterCandidate &candidate, idx_t max_threads = 0) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, candidate, nullptr, max_threads);
	});
	return result;
}

static duckdb::shared_ptr<TableReclusterState> GetStartTestState(Connection &con, const string &table_name) {
	duckdb::shared_ptr<TableReclusterState> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	return result;
}

static void CleanupStartedTask(const duckdb::shared_ptr<TableReclusterState> &state,
                               const duckdb::shared_ptr<RangeTask> &task) {
	task->RequestCancel();
	REQUIRE(task->TryEnterCancelling());
	task->GetTaskContext().CloseSnapshot();
	REQUIRE(!task->GetTaskContext().HasActiveSnapshot());
	REQUIRE(task->TryDetach());
	state->RemoveTask(task->GetTaskId());
}

TEST_CASE("Recluster task start revalidates and registers a read snapshot", "[storage][recluster_start]") {
	auto path = TestCreatePath("recluster_task_start.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS start_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE start_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(8192) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));

	auto candidate = SelectStartCandidate(con, "tbl", {4096, 2, 4, 0.25});
	auto result = StartCandidate(con, "tbl", candidate, 2);
	REQUIRE(result.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(result.task);
	REQUIRE(result.task->GetState() == RangeTaskState::PREPARING);
	REQUIRE(result.task->GetRange().start == 0);
	REQUIRE(result.task->GetRange().end == 4096);
	auto &task_context = result.task->GetTaskContext();
	REQUIRE(task_context.HasActiveSnapshot());
	REQUIRE(task_context.GetSnapshotStartTime() > 0);
	REQUIRE(task_context.GetSnapshotTransaction().start_time == task_context.GetSnapshotStartTime());
	REQUIRE(task_context.GetCandidate().expected_row_groups.size() == 2);
	REQUIRE(task_context.GetSortDefinition().sort_order_id == candidate.sort_order_id);
	REQUIRE(task_context.GetPhysicalSortIndexes().size() == 1);
	REQUIRE(task_context.GetPhysicalSortIndexes()[0] == 0);
	REQUIRE(task_context.GetThreadLimit(8) == 2);
	REQUIRE(task_context.GetThreadLimit(1) == 1);

	auto state = GetStartTestState(con, "tbl");
	REQUIRE(state);
	REQUIRE(state->GetTask(result.task->GetTaskId()).get() == result.task.get());
	auto reserved_ranges = state->GetReservedRanges();
	REQUIRE(reserved_ranges.size() == 1);
	REQUIRE(reserved_ranges[0].start == 0);
	REQUIRE(reserved_ranges[0].end == 4096);
	auto next_candidate = SelectStartCandidate(con, "tbl", {4096, 2, 4, 0.25});
	REQUIRE(next_candidate.range.start == 4096);
	REQUIRE(next_candidate.range.end == 8192);

	CleanupStartedTask(state, result.task);
	REQUIRE(state->GetReservedRanges().empty());
	DeleteDatabase(path);
}

TEST_CASE("Recluster task start waits for writers before taking its snapshot", "[storage][recluster_start]") {
	auto path = TestCreatePath("recluster_task_start_writer.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH '" + path + "' AS start_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(setup.Query("USE start_db"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT start_db"));
	REQUIRE_NO_FAIL(setup.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT start_db"));
	auto candidate = SelectStartCandidate(setup, "tbl", {4096, 2, 4, 0.25});

	Connection writer(db);
	REQUIRE_NO_FAIL(writer.Query("USE start_db"));
	writer.BeginTransaction();
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM tbl WHERE i = 0"));

	Connection starter(db);
	REQUIRE_NO_FAIL(starter.Query("USE start_db"));
	auto start_future = std::async(std::launch::async, [&]() { return StartCandidate(starter, "tbl", candidate); });
	REQUIRE(start_future.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
	writer.Commit();
	auto finished_status = start_future.wait_for(std::chrono::seconds(5));
	if (finished_status != std::future_status::ready) {
		starter.Interrupt();
	}
	REQUIRE(finished_status == std::future_status::ready);
	auto result = start_future.get();
	REQUIRE(result.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(result.task);
	REQUIRE(result.task->GetTaskContext().GetCandidate().input_live_rows == 4095);
	REQUIRE(result.task->GetTaskContext().GetCandidate().input_deleted_rows == 1);

	auto state = GetStartTestState(setup, "tbl");
	CleanupStartedTask(state, result.task);
	DeleteDatabase(path);
}

TEST_CASE("Recluster task start releases the table gate while a checkpoint is pending", "[storage][recluster_start]") {
	auto path = TestCreatePath("recluster_task_start_checkpoint.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH '" + path + "' AS start_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(setup.Query("USE start_db"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT start_db"));
	REQUIRE_NO_FAIL(setup.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT start_db"));
	auto candidate = SelectStartCandidate(setup, "tbl", {4096, 2, 4, 0.25});

	ReclusterManager *manager = nullptr;
	setup.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*setup.context, QualifiedName(Identifier("tbl")));
		manager = &entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager();
	});
	REQUIRE(manager);
	auto checkpoint_lock = manager->GetExclusiveLayoutPublishLock();

	Connection starter(db);
	REQUIRE_NO_FAIL(starter.Query("USE start_db"));
	auto start_future = std::async(std::launch::async, [&]() { return StartCandidate(starter, "tbl", candidate); });
	REQUIRE(start_future.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);

	Connection writer(db);
	REQUIRE_NO_FAIL(writer.Query("USE start_db"));
	auto writer_future =
	    std::async(std::launch::async, [&]() { return writer.Query("INSERT INTO tbl VALUES (5000)"); });
	auto writer_status = writer_future.wait_for(std::chrono::seconds(5));
	checkpoint_lock.reset();
	if (writer_status != std::future_status::ready) {
		writer.Interrupt();
	}
	REQUIRE(writer_status == std::future_status::ready);
	REQUIRE_NO_FAIL(writer_future.get());

	auto start_status = start_future.wait_for(std::chrono::seconds(5));
	if (start_status != std::future_status::ready) {
		starter.Interrupt();
	}
	REQUIRE(start_status == std::future_status::ready);
	auto result = start_future.get();
	REQUIRE(result.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(result.task);
	auto state = GetStartTestState(setup, "tbl");
	CleanupStartedTask(state, result.task);
	DeleteDatabase(path);
}

TEST_CASE("Recluster task start rejects a stale physical identity", "[storage][recluster_start]") {
	auto path = TestCreatePath("recluster_task_start_stale.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS start_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE start_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));
	auto candidate = SelectStartCandidate(con, "tbl", {4096, 2, 4, 0.25});

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto row_group = entry.GetStorage().GetRowGroupCollection()->GetRowGroup(0);
		REQUIRE(row_group);
		row_group->SetSortMetadata({candidate.sort_order_id, 100}, true);
	});
	auto result = StartCandidate(con, "tbl", candidate);
	REQUIRE(result.status == ReclusterTaskStartStatus::STALE_CANDIDATE);
	REQUIRE(!result.task);
	auto state = GetStartTestState(con, "tbl");
	REQUIRE(state->GetReservedRanges().empty());
	DeleteDatabase(path);
}

TEST_CASE("Explicit recluster reports an already active table task", "[storage][recluster_start]") {
	auto path = TestCreatePath("recluster_explicit_already_running.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS start_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE start_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT start_db"));

	auto candidate = SelectStartCandidate(con, "tbl", {4096, 2, 4, 0.25});
	auto started = StartCandidate(con, "tbl", candidate);
	REQUIRE(started.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(started.task);

	auto explicit_result =
	    con.Query("SELECT tasks_completed, state FROM recluster('start_db.main.tbl', max_bytes='16MB', max_tasks=1)");
	REQUIRE(explicit_result);
	REQUIRE(CHECK_COLUMN(explicit_result, 0, {0}));
	REQUIRE(CHECK_COLUMN(explicit_result, 1, {"ALREADY_RUNNING"}));

	auto state = GetStartTestState(con, "tbl");
	CleanupStartedTask(state, started.task);
	DeleteDatabase(path);
}
