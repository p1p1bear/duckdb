#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
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

TEST_CASE("Only successful checkpoints publish recluster candidates", "[storage][recluster_manager]") {
	auto path = TestCreatePath("recluster_manager_checkpoint.db");
	DeleteDatabase(path);
	duckdb::shared_ptr<TableReclusterState> state;
	optional<CheckpointLayoutSnapshot> successful_snapshot;
	{
		DuckDB db;
		Connection con(db);
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
