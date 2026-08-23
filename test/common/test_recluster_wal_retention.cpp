#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static AttachedDatabase &GetRetentionDatabase(DuckDB &db, Connection &con) {
	auto database_name = DatabaseManager::GetDefaultDatabase(*con.context);
	auto attached = db.instance->GetDatabaseManager().GetDatabase(database_name);
	REQUIRE(attached);
	return *attached;
}

static block_id_t CreateRetentionFreeBlock(SingleFileBlockManager &block_manager) {
	auto block_id = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsModified(block_id);
	return block_id;
}

TEST_CASE("Recluster WAL retention releases only obsolete generations", "[storage][recluster_wal_retention]") {
	auto path = TestCreatePath("recluster_wal_retention.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &attached = GetRetentionDatabase(db, con);
	auto &block_manager = attached.GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
	auto &retention = attached.GetReclusterManager().GetWALBlockRetention();

	auto first_block = block_manager.GetFreeBlockId();
	auto second_block = block_manager.GetFreeBlockId();
	auto current_block = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsModified(first_block);
	block_manager.MarkBlockAsModified(second_block);
	block_manager.MarkBlockAsModified(current_block);
	auto first = retention.Reserve({first_block}, {first_block});
	auto second = retention.Reserve({}, {second_block});
	auto current = retention.Reserve({current_block}, {});
	retention.Commit(std::move(first), {10, 100});
	retention.Commit(std::move(second), {10, 200});
	retention.Commit(std::move(current), {11, 50});
	REQUIRE(retention.Count() == 3);

	retention.ReleaseNoLongerReplayable({11, 0, 75});
	REQUIRE(retention.Count() == 1);
	REQUIRE(!block_manager.IsBlockReserved(first_block));
	REQUIRE(!block_manager.IsBlockReserved(second_block));
	REQUIRE(block_manager.IsBlockReserved(current_block));

	retention.ReleaseNoLongerReplayable({12, 0, 0});
	REQUIRE(retention.Count() == 0);
	REQUIRE(!block_manager.IsBlockReserved(current_block));
}

TEST_CASE("Successful checkpoints release obsolete recluster WAL retention", "[storage][recluster_wal_retention]") {
	auto path = TestCreatePath("recluster_wal_retention_checkpoint.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (42)"));
	auto &attached = GetRetentionDatabase(db, con);
	auto &storage_manager = attached.GetStorageManager();
	auto &block_manager = storage_manager.GetBlockManager().Cast<SingleFileBlockManager>();
	auto &retention = attached.GetReclusterManager().GetWALBlockRetention();
	auto checkpoint_iteration = block_manager.GetCheckpointIteration();
	auto wal_size = storage_manager.GetWALSize();
	REQUIRE(wal_size > 0);

	auto retained_block = CreateRetentionFreeBlock(block_manager);
	auto reservation = retention.Reserve({retained_block}, {});
	retention.Commit(std::move(reservation), {checkpoint_iteration, wal_size});
	REQUIRE(block_manager.IsBlockReserved(retained_block));
	REQUIRE(retention.Count() == 1);

	REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
	REQUIRE(block_manager.GetCheckpointIteration() == checkpoint_iteration + 1);
	REQUIRE(retention.Count() == 0);
	REQUIRE(!block_manager.IsBlockReserved(retained_block));
	REQUIRE(block_manager.PeekFreeBlockId() == retained_block);
}
