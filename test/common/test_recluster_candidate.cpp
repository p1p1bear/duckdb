#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<TableReclusterState> GetCandidateTestState(Connection &con, const string &table_name) {
	duckdb::shared_ptr<TableReclusterState> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	return result;
}

static ReclusterCandidateSelection SelectCandidateForTest(Connection &con, const string &table_name,
                                                          const ReclusterCandidateLimits &limits) {
	ReclusterCandidateSelection result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		result = SelectReclusterCandidate(*collection, entry.GetStorage().Columns(), *state, limits);
	});
	return result;
}

static void InstallCandidateTestRuns(Connection &con, const string &table_name,
                                     const duckdb::vector<sort_run_id_t> &run_ids) {
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		REQUIRE(collection->GetRowGroupCount() == run_ids.size());
		auto sort_order_id = state->GetCurrentSortOrderId();
		for (idx_t row_group_index = 0; row_group_index < run_ids.size(); row_group_index++) {
			auto row_group = collection->GetRowGroup(NumericCast<int64_t>(row_group_index));
			REQUIRE(row_group);
			row_group->SetSortMetadata({sort_order_id, run_ids[row_group_index]}, true);
		}
		auto snapshot = BuildCheckpointLayoutSnapshot(*collection, entry.GetStorage().Columns(), 99);
		REQUIRE(snapshot);
		REQUIRE(state->TryInstallCheckpointSnapshot(sort_order_id, collection->GetStorageGenerationId(),
		                                            std::move(*snapshot)));
	});
}

TEST_CASE("Recluster candidates convert the earliest checkpointed inputs", "[storage][recluster_candidate]") {
	auto path = TestCreatePath("recluster_conversion_candidate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS candidate_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE candidate_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(10240) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT candidate_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT candidate_db"));

	ReclusterCandidateLimits limits {6144, 3, 4, 0.25};
	auto selection = SelectCandidateForTest(con, "tbl", limits);
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
	REQUIRE(selection.candidate);
	REQUIRE(selection.candidate->type == ReclusterCandidateType::CONVERSION);
	REQUIRE(selection.candidate->range.start == 0);
	REQUIRE(selection.candidate->range.end == 6144);
	REQUIRE(selection.candidate->input_physical_rows == 6144);
	REQUIRE(selection.candidate->input_live_rows == 6144);
	REQUIRE(selection.candidate->input_deleted_rows == 0);
	REQUIRE(selection.candidate->row_group_count == 3);
	REQUIRE(selection.candidate->run_count == 0);
	REQUIRE(selection.candidate->expected_row_groups.size() == 3);

	auto state = GetCandidateTestState(con, "tbl");
	REQUIRE(state);
	REQUIRE(state->TryRegisterTask(make_shared_ptr<RangeTask>(recluster_task_id_t(0, 1), RowGroupRange {0, 2048})));
	selection = SelectCandidateForTest(con, "tbl", limits);
	REQUIRE(selection.candidate);
	REQUIRE(selection.candidate->range.start == 2048);
	REQUIRE(selection.candidate->range.end == 8192);
	state->RemoveTask(recluster_task_id_t(0, 1));

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		auto row_group = collection->GetRowGroup(0);
		REQUIRE(row_group);
		row_group->SetSortMetadata({state->GetCurrentSortOrderId(), 100}, true);
	});
	selection = SelectCandidateForTest(con, "tbl", limits);
	REQUIRE(selection.candidate);
	REQUIRE(selection.candidate->range.start == 2048);

	state->ClearLastCheckpoint();
	selection = SelectCandidateForTest(con, "tbl", limits);
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::NO_CHECKPOINTED_RANGE);
	REQUIRE(!selection.candidate);
	DeleteDatabase(path);
}

TEST_CASE("Recluster candidates preserve runs and prioritize delete cleanup", "[storage][recluster_candidate]") {
	auto path = TestCreatePath("recluster_run_candidate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS candidate_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE candidate_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(12288) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT candidate_db"));

	InstallCandidateTestRuns(con, "tbl", {41, 41, 41, 41, 41, 41});
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE i < 2048"));
	auto selection = SelectCandidateForTest(con, "tbl", {8192, 4, 4, 0.10});
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT);
	REQUIRE(!selection.candidate);

	selection = SelectCandidateForTest(con, "tbl", {12288, 6, 4, 0.10});
	REQUIRE(selection.candidate);
	REQUIRE(selection.candidate->type == ReclusterCandidateType::DELETE_CLEANUP);
	REQUIRE(selection.candidate->range.start == 0);
	REQUIRE(selection.candidate->range.end == 12288);
	REQUIRE(selection.candidate->input_physical_rows == 12288);
	REQUIRE(selection.candidate->input_live_rows == 10240);
	REQUIRE(selection.candidate->input_deleted_rows == 2048);
	REQUIRE(selection.candidate->row_group_count == 6);
	REQUIRE(selection.candidate->run_count == 1);

	InstallCandidateTestRuns(con, "tbl", {51, 51, 52, 53, 54, 55});
	selection = SelectCandidateForTest(con, "tbl", {8192, 4, 4, 1.0});
	REQUIRE(selection.candidate);
	REQUIRE(selection.candidate->type == ReclusterCandidateType::RUN_MERGE);
	REQUIRE(selection.candidate->range.start == 4096);
	REQUIRE(selection.candidate->range.end == 12288);
	REQUIRE(selection.candidate->row_group_count == 4);
	REQUIRE(selection.candidate->run_count == 4);

	InstallCandidateTestRuns(con, "tbl", {61, 61, 62, 62, 62, 62});
	auto state = GetCandidateTestState(con, "tbl");
	REQUIRE(state);
	REQUIRE(state->TryRegisterTask(make_shared_ptr<RangeTask>(recluster_task_id_t(0, 2), RowGroupRange {0, 2048})));
	selection = SelectCandidateForTest(con, "tbl", {12288, 6, 4, 1.0});
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::NO_ELIGIBLE_RANGE);
	REQUIRE(!selection.candidate);
	state->RemoveTask(recluster_task_id_t(0, 2));

	InstallCandidateTestRuns(con, "tbl", {71, 72, 71, 73, 74, 75});
	REQUIRE_THROWS_AS(SelectCandidateForTest(con, "tbl", {12288, 6, 4, 1.0}), InternalException);

	ReclusterCandidateLimits invalid_limits {12288, 6, 1, 0.10};
	REQUIRE_THROWS_AS(SelectCandidateForTest(con, "tbl", invalid_limits), InternalException);
	DeleteDatabase(path);
}
