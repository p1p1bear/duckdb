#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
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

static ReclusterTaskStartResult StartRunMergeTask(Connection &con, const string &table_name,
                                                  const ReclusterCandidateLimits &limits = {32768, 16, 4, 0.25}) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		ReclusterLayoutAnalysis analysis(*entry.GetStorage().GetRowGroupCollection(), entry.GetStorage().Columns(),
		                                 *state);
		auto selection = analysis.SelectCandidate(limits);
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

TEST_CASE("Recluster merge bounds variable-length source buffers across refills", "[storage][recluster_sort]") {
	bool interleaved = true;
	SECTION("Interleaved runs") {
	}
	SECTION("One run remains unread across repeated refills") {
		interleaved = false;
	}
	auto path = TestCreatePath("recluster_merge_buffers.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1; SET auto_recluster=false; SET memory_limit='64MB'"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS buffers (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE buffers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k BIGINT, payload VARCHAR, items BIGINT[]) SORTED BY(k)"));
	for (idx_t run = 0; run < 2; run++) {
		auto key = interleaved ? "i*2+" + std::to_string(run) : "i+" + std::to_string(run * 65536);
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT k, CASE WHEN k%31=0 THEN NULL ELSE repeat('x',128)||k END, "
		                          "CASE WHEN k%47=0 THEN NULL ELSE [k,-k] END FROM "
		                          "(SELECT " +
		                          key + " AS k FROM range(65536) t(i))"));
	}
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT buffers"));
	auto start = StartRunMergeTask(con, "tbl", {131072, 64, 4, 0.25});
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	ReclusterSorter sorter(*start.task);
	sorter.Prepare();
	DataChunk chunk;
	sorter.InitializeChunk(chunk);
	idx_t output_rows = 0;
	while (sorter.Scan(chunk)) {
		chunk.Verify();
		auto &strings = DictionaryVector::Child(chunk.data[1]);
		auto &items = DictionaryVector::Child(chunk.data[2]);
		REQUIRE(strings.GetAllocationSize() < 16 * 1024 * 1024);
		REQUIRE(ListVector::GetListSize(items) <= 128 * 1024);
		for (idx_t row = 0; row < chunk.size(); row++, output_rows++) {
			auto key = static_cast<int64_t>(output_rows);
			REQUIRE(chunk.GetValue(0, row) == Value::BIGINT(key));
			auto payload = key % 31 == 0 ? Value(LogicalType::VARCHAR) : Value(string(128, 'x') + std::to_string(key));
			REQUIRE(Value::NotDistinctFrom(chunk.GetValue(1, row), payload));
			auto list = key % 47 == 0 ? Value(LogicalType::LIST(LogicalType::BIGINT))
			                          : Value::LIST(LogicalType::BIGINT, {Value::BIGINT(key), Value::BIGINT(-key)});
			REQUIRE(Value::NotDistinctFrom(chunk.GetValue(2, row), list));
		}
	}
	REQUIRE(output_rows == 131072);
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
