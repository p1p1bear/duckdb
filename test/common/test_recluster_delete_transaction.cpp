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
#include "duckdb/transaction/duck_transaction.hpp"
#include "test_helpers.hpp"

#include <algorithm>

using namespace duckdb; // NOLINT

struct DeleteTransactionTask {
	ReclusterTaskStartResult start;
	duckdb::shared_ptr<TableReclusterState> state;
};

static DeleteTransactionTask StartDeleteTransactionTask(Connection &con) {
	DeleteTransactionTask result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		result.state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(result.state);
		auto selection = SelectReclusterCandidate(*entry.GetStorage().GetRowGroupCollection(),
		                                          entry.GetStorage().Columns(), *result.state, {4096, 2, 4, 0.25});
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		result.start = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, *selection.candidate);
	});
	REQUIRE(result.start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(result.start.task);
	return result;
}

static void CleanupDeleteTransactionTask(DeleteTransactionTask &task) {
	task.start.task->RequestCancel();
	REQUIRE(task.start.task->TryEnterCancelling());
	task.start.task->GetTaskContext().CloseSnapshot();
	REQUIRE(task.start.task->TryDetach());
	task.state->RemoveTask(task.start.task->GetTaskId());
}

static void RequireDeleteSlot(const ReclusterDeleteSlot &slot, DeleteSlotState state,
                              duckdb::vector<row_t> expected_rowids) {
	REQUIRE(slot.GetState() == state);
	auto actual_rowids = slot.GetOldRowIds();
	std::sort(actual_rowids.begin(), actual_rowids.end());
	std::sort(expected_rowids.begin(), expected_rowids.end());
	REQUIRE(actual_rowids == expected_rowids);
}

TEST_CASE("User DELETE commits exact row IDs to recluster tasks", "[storage][recluster_delete_transaction]") {
	auto path = TestCreatePath("recluster_delete_transaction.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(setup.Query("ATTACH '" + path + "' AS delete_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(setup.Query("USE delete_db"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(setup.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(8192) t(i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT delete_db"));
	REQUIRE_NO_FAIL(setup.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(setup.Query("CHECKPOINT delete_db"));
	auto task = StartDeleteTransactionTask(setup);

	Connection writer(db);
	REQUIRE_NO_FAIL(writer.Query("USE delete_db"));
	writer.BeginTransaction();
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM tbl WHERE i IN (3, 5)"));
	REQUIRE_NO_FAIL(
	    writer.Query("DELETE FROM tbl USING (VALUES (7), (7), (9)) duplicate(i) WHERE tbl.i = duplicate.i"));
	writer.Commit();

	auto committed = task.start.task->ScanResolvedDeletes(0, 8, 32);
	REQUIRE(committed.slots.size() == 1);
	REQUIRE(committed.resolved_through == 1);
	RequireDeleteSlot(committed.slots[0], DeleteSlotState::COMMITTED, {3, 5, 7, 9});
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM tbl WHERE i = 5000"));
	REQUIRE(task.start.task->GetLatestDeleteSequence() == 1);

	REQUIRE_NO_FAIL(writer.Query("SET debug_force_commit_failure=true"));
	writer.BeginTransaction();
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM tbl WHERE i IN (11, 13)"));
	auto failed_commit = writer.Query("COMMIT");
	REQUIRE_FAIL(failed_commit);
	REQUIRE(failed_commit->GetError().find("Forced commit failure") != string::npos);
	REQUIRE_NO_FAIL(writer.Query("SET debug_force_commit_failure=false"));

	auto resolved = task.start.task->ScanResolvedDeletes(0, 8, 32);
	REQUIRE(resolved.slots.size() == 2);
	REQUIRE(resolved.resolved_through == 2);
	RequireDeleteSlot(resolved.slots[1], DeleteSlotState::ABORTED, {11, 13});
	auto remaining = writer.Query("SELECT count(*) FROM tbl WHERE i IN (11, 13)");
	REQUIRE(remaining);
	REQUIRE(CHECK_COLUMN(remaining, 0, {2}));

	writer.BeginTransaction();
	REQUIRE_NO_FAIL(writer.Query("DELETE FROM tbl WHERE i = 15"));
	writer.Rollback();
	REQUIRE(task.start.task->GetLatestDeleteSequence() == 2);
	remaining = writer.Query("SELECT count(*) FROM tbl WHERE i = 15");
	REQUIRE(remaining);
	REQUIRE(CHECK_COLUMN(remaining, 0, {1}));

	CleanupDeleteTransactionTask(task);
	DeleteDatabase(path);
}

TEST_CASE("Recluster journal exhaustion cancels only the background task", "[storage][recluster_delete_transaction]") {
	auto path = TestCreatePath("recluster_delete_journal_exhaustion.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS delete_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE delete_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT delete_db"));

	duckdb::shared_ptr<TableReclusterState> state;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	REQUIRE(state);
	auto task = duckdb::make_shared_ptr<RangeTask>(hugeint_t(0, 1), RowGroupRange {0, 2048},
	                                               ReclusterDeleteJournalLimits {1, 1});
	REQUIRE(state->TryRegisterTask(task));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i IN (1, 2)"));
	REQUIRE(task->IsPublishForbidden());
	REQUIRE(task->IsCancelRequested());
	REQUIRE(task->GetLatestDeleteSequence() == 0);
	auto remaining = con.Query("SELECT count(*) FROM tbl WHERE i IN (1, 2)");
	REQUIRE(remaining);
	REQUIRE(CHECK_COLUMN(remaining, 0, {0}));

	REQUIRE(task->TryEnterCancelling());
	REQUIRE(task->TryDetach());
	state->RemoveTask(task->GetTaskId());
	DeleteDatabase(path);
}

TEST_CASE("Commit preflight restores sorted table write gates", "[storage][recluster_delete_transaction]") {
	auto path = TestCreatePath("recluster_commit_preflight.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS preflight_db (STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE preflight_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));

	con.BeginTransaction();
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (42)"));
	auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
	auto &transaction = DuckTransaction::Get(*con.context, entry.catalog);
	auto &table_info = *entry.GetStorage().GetDataTableInfo();
	REQUIRE(transaction.GetLocalStorage().HasReclusterTableStorage());
	REQUIRE(transaction.HoldsReclusterWriteLock(table_info));
	transaction.ReleaseReclusterWriteLocks();
	REQUIRE_FALSE(transaction.HoldsReclusterWriteLock(table_info));
	con.Commit();

	auto result = con.Query("SELECT * FROM tbl");
	REQUIRE(result);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	con.BeginTransaction();
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (84)"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i = 84"));
	auto &empty_transaction = DuckTransaction::Get(*con.context, entry.catalog);
	empty_transaction.ReleaseReclusterWriteLocks();
	con.Commit();
	result = con.Query("SELECT * FROM tbl");
	REQUIRE(result);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE ordinary(i INTEGER)"));
	con.BeginTransaction();
	REQUIRE_NO_FAIL(con.Query("INSERT INTO ordinary VALUES (1)"));
	auto &ordinary_entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("ordinary")));
	auto &ordinary_transaction = DuckTransaction::Get(*con.context, ordinary_entry.catalog);
	REQUIRE_FALSE(ordinary_transaction.GetLocalStorage().HasReclusterTableStorage());
	con.Rollback();
	DeleteDatabase(path);
}
