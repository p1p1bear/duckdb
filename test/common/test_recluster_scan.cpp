#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_range_scanner.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/row_id_remap_store.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Recluster row ID remaps preserve physical row group ranges", "[storage][recluster_scan]") {
	duckdb::vector<RowGroupPhysicalIdentity> row_groups(2);
	row_groups[0].start = 10;
	row_groups[0].count = 3;
	row_groups[1].start = 20;
	row_groups[1].count = 2;
	RowIdRemapStore remap(row_groups);
	REQUIRE(remap.GetPhysicalRowCount() == 5);
	REQUIRE(remap.GetAllocationSize() == 5 * sizeof(row_t));
	REQUIRE(remap.GetMappedCount() == 0);
	REQUIRE(remap.GetNewRowId(10) == INVALID_REMAP_ROW_ID);
	REQUIRE(remap.GetNewRowId(21) == INVALID_REMAP_ROW_ID);
	REQUIRE_THROWS_AS(remap.GetNewRowId(9), InternalException);
	REQUIRE_THROWS_AS(remap.GetNewRowId(13), InternalException);
	REQUIRE_THROWS_AS(remap.GetNewRowId(22), InternalException);

	remap.SetNewRowId(10, 100);
	remap.SetNewRowId(21, 101);
	REQUIRE(remap.GetNewRowId(10) == 100);
	REQUIRE(remap.GetNewRowId(21) == 101);
	REQUIRE(remap.GetMappedCount() == 2);
	REQUIRE_THROWS_AS(remap.SetNewRowId(10, 102), InternalException);
	REQUIRE_THROWS_AS(remap.SetNewRowId(11, INVALID_REMAP_ROW_ID), InternalException);

	row_groups[1].start = 12;
	REQUIRE_THROWS_AS(RowIdRemapStore(row_groups), InternalException);
}

static ReclusterTaskStartResult StartScanTask(Connection &con, const string &table_name) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		auto selection = SelectReclusterCandidate(*entry.GetStorage().GetRowGroupCollection(),
		                                          entry.GetStorage().Columns(), *state, {4096, 2, 4, 0.25});
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		result = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, *selection.candidate);
	});
	return result;
}

TEST_CASE("Recluster range scans use the STARTING snapshot and include old row IDs", "[storage][recluster_scan]") {
	auto path = TestCreatePath("recluster_range_scan.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS scan_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE scan_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER, 'value-' || i::VARCHAR FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT scan_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT scan_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i = 0"));

	auto start = StartScanTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	auto &task_context = start.task->GetTaskContext();
	REQUIRE(task_context.GetRowIdRemap().GetPhysicalRowCount() == 4096);
	REQUIRE(task_context.GetRowIdRemap().GetAllocationSize() == 4096 * sizeof(row_t));

	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i = 1"));
	ReclusterRangeScanner scanner(task_context);
	REQUIRE(scanner.GetOutputTypes().size() == 3);
	REQUIRE(scanner.GetOutputTypes()[0] == LogicalType::INTEGER);
	REQUIRE(scanner.GetOutputTypes()[1] == LogicalType::VARCHAR);
	REQUIRE(scanner.GetOutputTypes()[2] == LogicalType::BIGINT);

	DataChunk chunk;
	scanner.InitializeChunk(chunk);
	idx_t output_row = 0;
	while (scanner.Scan(chunk)) {
		chunk.Flatten();
		auto integers = FlatVector::GetData<int32_t>(chunk.data[0]);
		auto old_rowids = FlatVector::GetData<int64_t>(chunk.data[2]);
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			REQUIRE(old_rowids[row_index] > 0);
			REQUIRE(old_rowids[row_index] < 4096);
			REQUIRE(integers[row_index] == old_rowids[row_index]);
			REQUIRE(chunk.data[1].GetValue(row_index) == Value("value-" + std::to_string(old_rowids[row_index])));
			task_context.GetRowIdRemap().SetNewRowId(old_rowids[row_index], NumericCast<row_t>(output_row++));
		}
	}
	REQUIRE(scanner.GetScannedRowCount() == 4095);
	REQUIRE(output_row == 4095);
	REQUIRE(task_context.GetRowIdRemap().GetMappedCount() == 4095);
	REQUIRE(task_context.GetRowIdRemap().GetNewRowId(0) == INVALID_REMAP_ROW_ID);
	REQUIRE(task_context.GetRowIdRemap().GetNewRowId(1) == 0);
	REQUIRE(task_context.GetRowIdRemap().GetNewRowId(4095) == 4094);

	duckdb::shared_ptr<TableReclusterState> state;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	start.task->RequestCancel();
	REQUIRE(start.task->TryEnterCancelling());
	task_context.CloseSnapshot();
	REQUIRE(start.task->TryDetach());
	state->RemoveTask(start.task->GetTaskId());
	DeleteDatabase(path);
}
