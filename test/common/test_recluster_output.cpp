#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static ReclusterTaskStartResult StartOutputTask(Connection &con, const string &table_name) {
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

static void RemoveOutputTask(Connection &con, ReclusterTaskStartResult &start, const string &table_name) {
	duckdb::shared_ptr<TableReclusterState> state;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	start.task->RequestCancel();
	REQUIRE(start.task->TryEnterCancelling());
	start.task->GetTaskContext().CloseSnapshot();
	REQUIRE(start.task->TryDetach());
	state->RemoveTask(start.task->GetTaskId());
	start.task.reset();
}

class OutputTaskCleanupGuard {
public:
	OutputTaskCleanupGuard(Connection &con_p, ReclusterTaskStartResult &start_p, string table_name_p)
	    : con(con_p), start(start_p), table_name(std::move(table_name_p)) {
	}

	~OutputTaskCleanupGuard() {
		if (!start.task) {
			return;
		}
		try {
			duckdb::shared_ptr<TableReclusterState> state;
			con.context->RunFunctionInTransaction([&]() {
				auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
				state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
			});
			start.task->RequestCancel();
			start.task->TryEnterCancelling();
			start.task->GetTaskContext().CloseSnapshot();
			start.task->TryDetach();
			if (state) {
				state->RemoveTask(start.task->GetTaskId());
			}
			start.task.reset();
		} catch (...) {
		}
	}

private:
	Connection &con;
	ReclusterTaskStartResult &start;
	string table_name;
};

static void CheckOutputRows(Connection &con, ReclusterOutput &output, MaterializedQueryResult &expected) {
	auto &collection = *output.GetCollection();
	auto types = collection.GetTypes();
	duckdb::vector<StorageIndex> column_ids;
	for (idx_t column_index = 0; column_index < types.size(); column_index++) {
		column_ids.emplace_back(column_index);
	}

	TableScanState scan_state;
	scan_state.Initialize(column_ids, con.context.get());
	collection.InitializeScan(QueryContext(*con.context), scan_state.table_state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(*con.context, types);
	idx_t output_row = 0;
	while (true) {
		chunk.Reset();
		if (!scan_state.table_state.Scan(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
			break;
		}
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			for (idx_t column_index = 0; column_index < chunk.ColumnCount(); column_index++) {
				INFO("output row " << output_row << ", column " << column_index << ", actual "
				                   << chunk.GetValue(column_index, row_index).ToString() << ", expected "
				                   << expected.GetValue(column_index, output_row).ToString());
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(column_index, row_index),
				                               expected.GetValue(column_index, output_row)));
			}
			output_row++;
		}
	}
	REQUIRE(output_row == expected.RowCount());
}

TEST_CASE("Recluster output writes sorted task-private row groups", "[storage][recluster_output]") {
	auto path = TestCreatePath("recluster_output.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS output_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE output_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k1 INTEGER, k2 VARCHAR, payload BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT CASE WHEN i % 23 = 0 THEN NULL ELSE (i * 37) % 4096 END, "
	                          "CASE WHEN i % 29 = 0 AND i % 23 <> 0 THEN NULL "
	                          "ELSE lpad(i::VARCHAR, 4, '0') END, i * 101 FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k1, k2)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_db"));
	auto expected_result = con.Query("SELECT * FROM tbl ORDER BY k1 ASC NULLS LAST, k2 ASC NULLS LAST");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	auto &expected = expected_result->Cast<MaterializedQueryResult>();

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	auto &storage = *start.task->GetTaskContext().GetStorage();
	auto &block_manager = storage.GetTableIOManager().GetBlockManagerForRowData();
	auto first_private_block = block_manager.PeekFreeBlockId();

	ReclusterOutputWriter writer(*start.task);
	try {
		writer.Write();
	} catch (...) {
		RemoveOutputTask(con, start, "tbl");
		throw;
	}
	auto &task_context = start.task->GetTaskContext();
	REQUIRE(task_context.HasOutput());
	REQUIRE(!task_context.HasActiveSnapshot());
	auto &output = task_context.GetOutput();
	REQUIRE(output.GetSortOrderId() == task_context.GetSortDefinition().sort_order_id);
	REQUIRE(output.GetRunId() != INVALID_SORT_RUN_ID);
	REQUIRE(output.GetRowCount() == 4096);
	REQUIRE(output.GetCollection()->GetTotalRows() == 4096);
	REQUIRE(output.GetCollection()->GetNextRowId() == 4096);
	REQUIRE(output.GetPersistentData().row_group_data.size() == 2);
	REQUIRE(output.GetBlockIds().size() >= 6);
	REQUIRE(std::find(output.GetBlockIds().begin(), output.GetBlockIds().end(), first_private_block) !=
	        output.GetBlockIds().end());
	REQUIRE(std::find(output.GetBlockIds().begin(), output.GetBlockIds().end(), block_manager.PeekFreeBlockId()) ==
	        output.GetBlockIds().end());

	auto output_groups = output.GetRowGroups();
	REQUIRE(output_groups.size() == 2);
	for (auto &row_group : output_groups) {
		REQUIRE(row_group->IsPersistent());
		REQUIRE(row_group->IsSealed());
		REQUIRE(row_group->GetSortMetadata() == RowGroupSortMetadata {output.GetSortOrderId(), output.GetRunId()});
		REQUIRE(!row_group->GetOrCreateVersionInfo().HasUncommittedChanges());
	}
	auto output_tree = output.GetCollection()->GetRowGroups();
	auto first_group = output_tree->GetRootSegment();
	REQUIRE(first_group);
	REQUIRE(first_group->GetRowStart() == 0);
	REQUIRE(first_group->GetNode().count == 2048);
	auto second_group = output_tree->GetNextSegment(*first_group);
	REQUIRE(second_group);
	REQUIRE(second_group->GetRowStart() == 2048);
	REQUIRE(second_group->GetNode().count == 2048);
	REQUIRE(!output_tree->GetNextSegment(*second_group));
	CheckOutputRows(con, output, expected);

	auto main_result = con.Query("SELECT count(*), count(DISTINCT payload) FROM tbl");
	REQUIRE_NO_FAIL(*main_result);
	REQUIRE(CHECK_COLUMN(main_result, 0, {4096}));
	REQUIRE(CHECK_COLUMN(main_result, 1, {4096}));
	output_groups.clear();
	output_tree.reset();
	RemoveOutputTask(con, start, "tbl");
	REQUIRE(block_manager.PeekFreeBlockId() == first_private_block);
	DeleteDatabase(path);
}

TEST_CASE("Recluster output supports an empty replacement", "[storage][recluster_output]") {
	auto path = TestCreatePath("recluster_output_empty.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS output_empty_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl"));

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	ReclusterOutputWriter writer(*start.task);
	try {
		writer.Write();
	} catch (...) {
		RemoveOutputTask(con, start, "tbl");
		throw;
	}
	auto &output = start.task->GetTaskContext().GetOutput();
	REQUIRE(output.GetRowCount() == 0);
	REQUIRE(output.GetCollection()->GetRowGroupCount() == 0);
	REQUIRE(output.GetPersistentData().row_group_data.empty());
	REQUIRE(output.GetBlockIds().empty());
	REQUIRE(output.GetRowGroups().empty());
	REQUIRE(!start.task->GetTaskContext().HasActiveSnapshot());

	RemoveOutputTask(con, start, "tbl");
	DeleteDatabase(path);
}
