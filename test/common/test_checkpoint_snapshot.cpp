#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Checkpoint row group identities ignore delete metadata", "[storage][checkpoint_snapshot]") {
	auto path = TestCreatePath("recluster_checkpoint_identity.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS identity_test (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE identity_test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER, payload VARCHAR) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER, i::VARCHAR FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT identity_test"));

	optional<RowGroupPhysicalIdentity> before_delete;
	uint64_t storage_generation_id = 0;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		storage_generation_id = collection->GetStorageGenerationId();
		auto snapshot = BuildCheckpointLayoutSnapshot(*collection, entry.GetStorage().Columns(), 7);
		REQUIRE(snapshot);
		REQUIRE(snapshot->checkpoint_number == 7);
		REQUIRE(snapshot->storage_generation_id == storage_generation_id);
		REQUIRE(snapshot->row_groups.size() == 2);
		before_delete = snapshot->row_groups[0];
		REQUIRE(before_delete->format_version == 1);
		REQUIRE(before_delete->start == 0);
		REQUIRE(before_delete->count == 2048);
		REQUIRE(before_delete->sealed);
		REQUIRE(before_delete->sort_metadata.IsSorted());
		REQUIRE(before_delete->columns.size() == 2);
		REQUIRE(before_delete->columns[0].column_id == 1);
		REQUIRE(before_delete->columns[1].column_id == 2);
		REQUIRE(before_delete->immutable_data_checksum != 0);
		REQUIRE(ComputeRowGroupPhysicalIdentityChecksumV1(*before_delete) == before_delete->immutable_data_checksum);
	});

	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i = 1"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT identity_test"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetStorageGenerationId() == storage_generation_id);
		auto tree = collection->GetRowGroups();
		auto root = tree->GetRootSegment();
		REQUIRE(root);
		auto after_delete = ComputeRowGroupPhysicalIdentityV1(root->GetNode(), 0, entry.GetStorage().Columns());
		REQUIRE(after_delete);
		REQUIRE(*after_delete == *before_delete);

		auto changed = *after_delete;
		changed.columns[0].type = LogicalType::BIGINT;
		REQUIRE(ComputeRowGroupPhysicalIdentityChecksumV1(changed) != changed.immutable_data_checksum);
		changed = *after_delete;
		changed.sort_metadata.run_id++;
		REQUIRE(ComputeRowGroupPhysicalIdentityChecksumV1(changed) != changed.immutable_data_checksum);
	});
	DeleteDatabase(path);
}

TEST_CASE("Row group collection storage generations are process local", "[storage][checkpoint_snapshot]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE first(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE second(i INTEGER)"));
	uint64_t first_generation = 0;
	uint64_t second_generation = 0;
	con.context->RunFunctionInTransaction([&]() {
		auto &first = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("first")));
		auto &second = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("second")));
		first_generation = first.GetStorage().GetRowGroupCollection()->GetStorageGenerationId();
		second_generation = second.GetStorage().GetRowGroupCollection()->GetStorageGenerationId();
	});
	REQUIRE(first_generation != 0);
	REQUIRE(second_generation != 0);
	REQUIRE(first_generation != second_generation);
}

TEST_CASE("Checkpoint identities support lazy-loaded persistent columns", "[storage][checkpoint_snapshot]") {
	auto path = TestCreatePath("recluster_checkpoint_lazy_columns.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS lazy_test (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE lazy_test"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER, payload VARCHAR) SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER, i::VARCHAR FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT lazy_test"));
	}
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS lazy_test"));
		REQUIRE_NO_FAIL(con.Query("USE lazy_test"));
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			auto snapshot = BuildCheckpointLayoutSnapshot(*collection, entry.GetStorage().Columns(), 0);
			REQUIRE(snapshot);
			REQUIRE(snapshot->row_groups.size() == 2);
			REQUIRE(snapshot->row_groups[0].columns.size() == 2);
		});
	}
	DeleteDatabase(path);
}
