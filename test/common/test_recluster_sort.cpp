#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_sorter.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static ReclusterTaskStartResult StartSortTask(Connection &con, const string &table_name) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		ReclusterLayoutAnalysis analysis(*entry.GetStorage().GetRowGroupCollection(), entry.GetStorage().Columns(),
		                                 *state);
		auto selection = analysis.SelectCandidate({4096, 2, 4, 0.25});
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		result = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, *selection.candidate);
	});
	return result;
}

static ReclusterTaskStartResult StartRunMergeTask(Connection &con, const string &table_name) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		ReclusterLayoutAnalysis analysis(*entry.GetStorage().GetRowGroupCollection(), entry.GetStorage().Columns(),
		                                 *state);
		auto selection = analysis.SelectCandidate({32768, 16, 4, 0.25});
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		REQUIRE(selection.candidate->type == ReclusterCandidateType::RUN_MERGE);
		result = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, *selection.candidate);
	});
	return result;
}

static void RemoveSortTask(Connection &con, ReclusterTaskStartResult &start) {
	duckdb::shared_ptr<TableReclusterState> state;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	start.task->RequestCancel();
	REQUIRE(start.task->TryEnterCancelling());
	start.task->GetTaskContext().CloseSnapshot();
	REQUIRE(start.task->TryDetach());
	state->RemoveTask(start.task->GetTaskId());
}

TEST_CASE("Recluster sorter orders snapshot rows and builds the row ID remap", "[storage][recluster_sort]") {
	auto path = TestCreatePath("recluster_sort.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS sort_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE sort_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k1 INTEGER, k2 VARCHAR, payload BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT CASE WHEN i % 23 = 0 THEN NULL ELSE (i * 37) % 4096 END, "
	                          "CASE WHEN i % 29 = 0 AND i % 23 <> 0 THEN NULL "
	                          "ELSE lpad(i::VARCHAR, 4, '0') END, i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT sort_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k1, k2)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT sort_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload % 257 = 0"));
	auto expected_result = con.Query("SELECT k1, k2, payload, rowid FROM tbl ORDER BY k1 ASC NULLS LAST, "
	                                 "k2 ASC NULLS LAST, rowid");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	auto &expected = expected_result->Cast<MaterializedQueryResult>();

	auto start = StartSortTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload = 7"));

	ReclusterSorter sorter(*start.task);
	sorter.Prepare();
	REQUIRE(sorter.GetInputRowCount() == expected.RowCount());
	REQUIRE(!sorter.IsFinished());
	DataChunk chunk;
	sorter.InitializeChunk(chunk);
	idx_t output_row = 0;
	while (sorter.Scan(chunk)) {
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			for (idx_t column_index = 0; column_index < chunk.ColumnCount(); column_index++) {
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(column_index, row_index),
				                               expected.GetValue(column_index, output_row)));
			}
			auto old_rowid = chunk.GetValue(3, row_index).GetValue<row_t>();
			REQUIRE(start.task->GetTaskContext().GetRowIdRemap().GetNewRowId(old_rowid) ==
			        NumericCast<row_t>(output_row));
			output_row++;
		}
	}
	REQUIRE(output_row == expected.RowCount());
	REQUIRE(sorter.GetSortedRowCount() == expected.RowCount());
	REQUIRE(sorter.IsFinished());
	REQUIRE(!start.task->GetTaskContext().HasActiveSnapshot());
	REQUIRE(start.task->GetTaskContext().GetRowIdRemap().GetNewRowId(0) == INVALID_REMAP_ROW_ID);

	RemoveSortTask(con, start);
	DeleteDatabase(path);
}

TEST_CASE("Recluster sorter handles a snapshot with no live rows", "[storage][recluster_sort]") {
	auto path = TestCreatePath("recluster_sort_empty.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS sort_empty_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE sort_empty_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT sort_empty_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT sort_empty_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl"));

	auto start = StartSortTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	ReclusterSorter sorter(*start.task);
	sorter.Prepare();
	REQUIRE(sorter.GetInputRowCount() == 0);
	REQUIRE(sorter.GetSortedRowCount() == 0);
	REQUIRE(sorter.IsFinished());
	REQUIRE(!start.task->GetTaskContext().HasActiveSnapshot());
	DataChunk chunk;
	sorter.InitializeChunk(chunk);
	REQUIRE(!sorter.Scan(chunk));
	REQUIRE(start.task->GetTaskContext().GetRowIdRemap().GetMappedCount() == 0);

	RemoveSortTask(con, start);
	DeleteDatabase(path);
}

TEST_CASE("Recluster sorter streams existing sorted runs", "[storage][recluster_sort]") {
	auto path = TestCreatePath("recluster_streaming_merge.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS merge_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE merge_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k1 INTEGER, k2 VARCHAR, payload BIGINT, "
	                          "nested_payload STRUCT(id BIGINT, tags VARCHAR[])) SORTED BY (k1, k2)"));
	for (idx_t run_index = 0; run_index < 3; run_index++) {
		auto run = std::to_string(run_index);
		auto query = "INSERT INTO tbl SELECT CASE WHEN i % 31 = 0 THEN NULL ELSE ((i * 3 + " + run +
		             ") % 2048)::INTEGER END, CASE WHEN i % 47 = 0 THEN NULL ELSE "
		             "lpad(((i + " +
		             run + ") % 97)::VARCHAR, 3, '0') END, (" + run +
		             " * 10000 + i)::BIGINT, {'id': i, 'tags': [i::VARCHAR, " + run +
		             "::VARCHAR]} FROM range(4096) t(i)";
		REQUIRE_NO_FAIL(con.Query(query));
	}
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT merge_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload % 997 = 0"));
	auto expected_result = con.Query("SELECT k1, k2, payload, nested_payload, rowid FROM tbl "
	                                 "ORDER BY k1 ASC NULLS LAST, k2 ASC NULLS LAST, rowid");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	auto &expected = expected_result->Cast<MaterializedQueryResult>();

	auto start = StartRunMergeTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload = 17"));

	ReclusterSorter sorter(*start.task);
	sorter.Prepare();
	REQUIRE(sorter.UsesStreamingMerge());
	REQUIRE(sorter.GetInputRowCount() == expected.RowCount());
	DataChunk chunk;
	sorter.InitializeChunk(chunk);
	idx_t output_row = 0;
	while (sorter.Scan(chunk)) {
		REQUIRE(chunk.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR);
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			for (idx_t column_index = 0; column_index < chunk.ColumnCount(); column_index++) {
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(column_index, row_index),
				                               expected.GetValue(column_index, output_row)));
			}
			auto old_rowid = chunk.GetValue(4, row_index).GetValue<row_t>();
			REQUIRE(start.task->GetTaskContext().GetRowIdRemap().GetNewRowId(old_rowid) ==
			        start.task->GetRange().start + NumericCast<row_t>(output_row));
			output_row++;
		}
	}
	REQUIRE(output_row == expected.RowCount());
	REQUIRE(sorter.GetSortedRowCount() == expected.RowCount());
	REQUIRE(sorter.IsFinished());

	RemoveSortTask(con, start);
	DeleteDatabase(path);
}

TEST_CASE("Recluster sorter streams delete cleanup for one sorted run", "[storage][recluster_sort]") {
	auto path = TestCreatePath("recluster_streaming_cleanup.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS cleanup_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE cleanup_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i BIGINT, payload VARCHAR) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (4095 - i)::BIGINT, 'value-' || i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT cleanup_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i % 2 = 0"));
	auto expected_result = con.Query("SELECT i, payload, rowid FROM tbl ORDER BY i, rowid");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	auto &expected = expected_result->Cast<MaterializedQueryResult>();

	auto start = StartSortTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	REQUIRE(start.task->GetTaskContext().GetCandidate().type == ReclusterCandidateType::DELETE_CLEANUP);
	ReclusterSorter sorter(*start.task);
	sorter.Prepare();
	REQUIRE(sorter.UsesStreamingMerge());
	DataChunk chunk;
	sorter.InitializeChunk(chunk);
	idx_t output_row = 0;
	while (sorter.Scan(chunk)) {
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			for (idx_t column_index = 0; column_index < chunk.ColumnCount(); column_index++) {
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(column_index, row_index),
				                               expected.GetValue(column_index, output_row)));
			}
			output_row++;
		}
	}
	REQUIRE(output_row == expected.RowCount());
	REQUIRE(sorter.GetSortedRowCount() == expected.RowCount());

	RemoveSortTask(con, start);
	DeleteDatabase(path);
}
