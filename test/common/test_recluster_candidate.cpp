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
		ReclusterLayoutAnalysis analysis(*collection, entry.GetStorage().Columns(), *state, optional_idx(0));
		result = analysis.SelectCandidate(limits);
	});
	return result;
}

static duckdb::vector<ReclusterCandidateSelection>
SelectCandidatesFromOneAnalysisForTest(Connection &con, const string &table_name,
                                       const duckdb::vector<ReclusterCandidateLimits> &limits) {
	duckdb::vector<ReclusterCandidateSelection> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		ReclusterLayoutAnalysis analysis(*collection, entry.GetStorage().Columns(), state->GetSchedulingSnapshot(),
		                                 optional_idx(0));
		for (auto &candidate_limits : limits) {
			result.push_back(analysis.SelectCandidate(candidate_limits));
		}
		REQUIRE(analysis.GetCheckpointRowGroupCount() == collection->GetRowGroupCount());
		REQUIRE(analysis.GetRowGroups().size() == collection->GetRowGroupCount());
		for (auto &row_group : analysis.GetRowGroups()) {
			REQUIRE(row_group.live_rows <= row_group.physical_rows);
		}
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
		REQUIRE(
		    state->TryInstallCheckpointSnapshot(sort_order_id, collection->GetStorageGenerationId(),
		                                        make_shared_ptr<const CheckpointLayoutSnapshot>(std::move(*snapshot))));
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
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		ReclusterLayoutAnalysis analysis(*collection, entry.GetStorage().Columns(), *state);
		REQUIRE(analysis.GetRowGroups().size() >= 2);
		REQUIRE(!analysis.IsCheckpointedRowGroup(0));
		REQUIRE(analysis.IsCheckpointedRowGroup(1));
	});

	state->ClearLastCheckpoint();
	selection = SelectCandidateForTest(con, "tbl", limits);
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::NO_CHECKPOINTED_RANGE);
	REQUIRE(!selection.candidate);
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		ReclusterLayoutAnalysis analysis(*collection, entry.GetStorage().Columns(), *state);
		REQUIRE(analysis.GetCheckpointRowGroupCount() == 0);
		REQUIRE(analysis.GetRowGroups().size() == collection->GetRowGroupCount());
		for (auto &row_group : analysis.GetRowGroups()) {
			REQUIRE(analysis.RequiresRewrite(row_group));
		}
	});
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
	auto selections = SelectCandidatesFromOneAnalysisForTest(
	    con, "tbl", {{8192, 4, 4, 0.10}, {12288, 6, 4, 0.10}, {8192, 4, 4, 0.10}, {12288, 6, 4, 0.10}});
	REQUIRE(selections[0].status == selections[2].status);
	REQUIRE(!selections[2].candidate);
	REQUIRE(selections[1].status == selections[3].status);
	REQUIRE(selections[1].candidate);
	REQUIRE(selections[3].candidate);
	REQUIRE(selections[1].candidate->type == selections[3].candidate->type);
	REQUIRE(selections[1].candidate->range.start == selections[3].candidate->range.start);
	REQUIRE(selections[1].candidate->range.end == selections[3].candidate->range.end);
	REQUIRE(selections[1].candidate->input_physical_rows == selections[3].candidate->input_physical_rows);
	REQUIRE(selections[1].candidate->input_live_rows == selections[3].candidate->input_live_rows);
	REQUIRE(selections[1].candidate->input_deleted_rows == selections[3].candidate->input_deleted_rows);
	REQUIRE(selections[1].candidate->row_group_count == selections[3].candidate->row_group_count);
	REQUIRE(selections[1].candidate->run_count == selections[3].candidate->run_count);
	auto selection = std::move(selections[0]);
	REQUIRE(selection.status == ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT);
	REQUIRE(!selection.candidate);

	selection = std::move(selections[1]);
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

TEST_CASE("Incremental run merges prioritize overlapping key ranges", "[storage][recluster_candidate]") {
	auto path = TestCreatePath("recluster_overlap_candidate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS candidate_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE candidate_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (10000 + i)::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (10000 + i)::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (30000 + i)::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (40000 + i)::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (50000 + i)::INTEGER FROM range(2048) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT candidate_db"));

	ReclusterCandidateLimits incremental {4096, 2, 2, 1.0};
	ReclusterCandidateLimits full = incremental;
	full.prioritize_overlap = false;
	auto selections = SelectCandidatesFromOneAnalysisForTest(con, "tbl", {incremental, full});
	REQUIRE(selections[0].candidate);
	REQUIRE(selections[0].candidate->type == ReclusterCandidateType::RUN_MERGE);
	REQUIRE(selections[0].candidate->range.start == 2048);
	REQUIRE(selections[0].candidate->range.end == 6144);
	REQUIRE(selections[1].candidate);
	REQUIRE(selections[1].candidate->type == ReclusterCandidateType::RUN_MERGE);
	REQUIRE(selections[1].candidate->range.start == 0);
	REQUIRE(selections[1].candidate->range.end == 4096);
	DeleteDatabase(path);
}
