#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<TableReclusterState> GetReclusterState(Connection &con, const string &table_name) {
	duckdb::shared_ptr<TableReclusterState> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	return result;
}

static ReclusterManager &GetReclusterManager(Connection &con, const string &table_name) {
	ReclusterManager *result = nullptr;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = &entry.GetStorage().GetAttached().GetReclusterManager();
	});
	REQUIRE(result);
	return *result;
}

TEST_CASE("Only successful checkpoints publish recluster candidates", "[storage][recluster_manager]") {
	auto path = TestCreatePath("recluster_manager_checkpoint.db");
	DeleteDatabase(path);
	duckdb::shared_ptr<TableReclusterState> state;
	optional<CheckpointLayoutSnapshot> successful_snapshot;
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS manager_test (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE manager_test"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
		REQUIRE(!GetReclusterState(con, "tbl"));

		REQUIRE_NO_FAIL(con.Query("CHECKPOINT manager_test"));
		state = GetReclusterState(con, "tbl");
		REQUIRE(state);
		REQUIRE(state->AcceptsNewTasks());
		successful_snapshot = state->GetLastCheckpoint();
		REQUIRE(successful_snapshot);
		REQUIRE(successful_snapshot->checkpoint_number > 0);
		REQUIRE(successful_snapshot->row_groups.size() == 2);

		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096, 6144) t(i)"));
		REQUIRE_NO_FAIL(con.Query("SET debug_checkpoint_abort = 'before_header_non_fatal'"));
		REQUIRE_FAIL(con.Query("CHECKPOINT manager_test"));
		REQUIRE(state->GetLastCheckpoint() == successful_snapshot);
	}
	DeleteDatabase(path);
}

TEST_CASE("Recovery distinguishes checkpoint candidates from WAL-only changes", "[storage][recluster_manager]") {
	auto path = TestCreatePath("recluster_manager_recovery.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("SET checkpoint_threshold = '10 GB'"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS recovered (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE recovered"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE checkpoint_sorted(i INTEGER) SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE wal_sorted(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO checkpoint_sorted SELECT i::INTEGER FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO wal_sorted SELECT i::INTEGER FROM range(2048) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT recovered"));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO checkpoint_sorted SELECT i::INTEGER FROM range(4096, 6144) t(i)"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE wal_sorted SET SORTED BY (i)"));
	}
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS recovered"));
		REQUIRE_NO_FAIL(con.Query("USE recovered"));

		auto checkpoint_state = GetReclusterState(con, "checkpoint_sorted");
		REQUIRE(checkpoint_state);
		auto checkpoint_snapshot = checkpoint_state->GetLastCheckpoint();
		REQUIRE(checkpoint_snapshot);
		REQUIRE(checkpoint_snapshot->checkpoint_number == 0);
		REQUIRE(checkpoint_snapshot->row_groups.size() == 2);
		auto count_result = con.Query("SELECT count(*) FROM checkpoint_sorted");
		REQUIRE(CHECK_COLUMN(count_result, 0, {6144}));

		auto wal_state = GetReclusterState(con, "wal_sorted");
		REQUIRE(wal_state);
		REQUIRE(wal_state->AcceptsNewTasks());
		REQUIRE(!wal_state->GetLastCheckpoint());

		REQUIRE_NO_FAIL(con.Query("CHECKPOINT recovered"));
		checkpoint_snapshot = checkpoint_state->GetLastCheckpoint();
		REQUIRE(checkpoint_snapshot);
		REQUIRE(checkpoint_snapshot->checkpoint_number > 0);
		idx_t checkpoint_rows = 0;
		for (auto &row_group : checkpoint_snapshot->row_groups) {
			checkpoint_rows += row_group.count;
		}
		REQUIRE(checkpoint_rows == 6144);
		auto wal_snapshot = wal_state->GetLastCheckpoint();
		REQUIRE(wal_snapshot);
		REQUIRE(wal_snapshot->checkpoint_number == checkpoint_snapshot->checkpoint_number);
		idx_t wal_rows = 0;
		for (auto &row_group : wal_snapshot->row_groups) {
			REQUIRE(row_group.sealed);
			REQUIRE(!row_group.sort_metadata.IsSorted());
			wal_rows += row_group.count;
		}
		REQUIRE(wal_rows == 2048);
	}
	DeleteDatabase(path);
}

TEST_CASE("Successful checkpoints schedule automatic recluster work", "[storage][recluster_auto]") {
	auto path = TestCreatePath("recluster_auto_checkpoint.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS auto_checkpoint (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE auto_checkpoint"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT 4095 - i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_checkpoint"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_checkpoint"));

	GetReclusterManager(con, "tbl").WaitForAutoRecluster();
	auto inversions =
	    con.Query("SELECT count(*) FROM (SELECT i, lag(i) OVER () previous_i FROM tbl) WHERE i < previous_i");
	REQUIRE(CHECK_COLUMN(inversions, 0, {0}));
	auto remaining = con.Query("SELECT tasks_completed, state FROM recluster('auto_checkpoint.main.tbl')");
	REQUIRE(CHECK_COLUMN(remaining, 0, {0}));
	REQUIRE(CHECK_COLUMN(remaining, 1, {"COMPLETE"}));
	DeleteDatabase(path);
}

TEST_CASE("Maintenance commits chain automatic work across task limits", "[storage][recluster_auto]") {
	auto path = TestCreatePath("recluster_auto_task_chain.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS auto_chain (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE auto_chain"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT 69999 - i FROM range(70000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_chain"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_chain"));

	GetReclusterManager(con, "tbl").WaitForAutoRecluster();
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto layout = entry.GetStorage().GetRowGroupCollection()->GetCurrentLayout();
		REQUIRE(layout);
		REQUIRE(layout->layout_version == 2);
		REQUIRE(layout->patches.size() == 2);
		REQUIRE(layout->patches[0]->range.end == layout->patches[1]->range.start);
	});
	DeleteDatabase(path);
}

TEST_CASE("Commits wake automatic recluster after it is enabled", "[storage][recluster_auto]") {
	auto path = TestCreatePath("recluster_auto_commit.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS auto_commit (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE auto_commit"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE wake(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT 4095 - i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_commit"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_commit"));

	auto &manager = GetReclusterManager(con, "tbl");
	manager.WaitForAutoRecluster();
	auto before =
	    con.Query("SELECT count(*) > 0 FROM (SELECT i, lag(i) OVER () previous_i FROM tbl) WHERE i < previous_i");
	REQUIRE(CHECK_COLUMN(before, 0, {true}));

	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=true"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO wake VALUES (1)"));
	manager.WaitForAutoRecluster();
	auto after = con.Query("SELECT count(*) FROM (SELECT i, lag(i) OVER () previous_i FROM tbl) WHERE i < previous_i");
	REQUIRE(CHECK_COLUMN(after, 0, {0}));
	DeleteDatabase(path);
}

TEST_CASE("Database close drains automatic recluster work", "[storage][recluster_auto]") {
	auto path = TestCreatePath("recluster_auto_close.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET threads=4"));
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS auto_close (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE auto_close"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE wake(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT 4095 - i FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_close"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_close"));
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=true"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO wake VALUES (1)"));
	}
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS auto_close"));
		REQUIRE_NO_FAIL(con.Query("USE auto_close"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_close"));
		auto rows = con.Query("SELECT count(*), sum(i) FROM tbl");
		REQUIRE(CHECK_COLUMN(rows, 0, {4096}));
		REQUIRE(CHECK_COLUMN(rows, 1, {8386560}));
	}
	DeleteDatabase(path);
}

TEST_CASE("Recovered checkpoint snapshots wait for a new checkpoint before automatic work",
          "[storage][recluster_auto]") {
	auto path = TestCreatePath("recluster_auto_recovery_snapshot.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET threads=4"));
		REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS auto_recovery (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE auto_recovery"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT 4095 - i FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_recovery"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_recovery"));
		GetReclusterManager(con, "tbl").WaitForAutoRecluster();
	}
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET threads=4"));
		REQUIRE_NO_FAIL(con.Query("SET debug_skip_checkpoint_on_commit=true"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS auto_recovery"));
		REQUIRE_NO_FAIL(con.Query("USE auto_recovery"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE wake(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO wake VALUES (1)"));
		auto &manager = GetReclusterManager(con, "tbl");
		manager.WaitForAutoRecluster();
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto layout = entry.GetStorage().GetRowGroupCollection()->GetCurrentLayout();
			REQUIRE(layout);
			REQUIRE(layout->layout_version == 1);
			REQUIRE(layout->patches.size() == 1);
		});

		REQUIRE_NO_FAIL(con.Query("CHECKPOINT auto_recovery"));
		manager.WaitForAutoRecluster();
		auto remaining = con.Query("SELECT tasks_completed, state FROM recluster('auto_recovery.main.tbl')");
		REQUIRE(CHECK_COLUMN(remaining, 0, {0}));
		REQUIRE(CHECK_COLUMN(remaining, 1, {"COMPLETE"}));
	}
	DeleteDatabase(path);
}
