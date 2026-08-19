#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<RowGroupSegmentTree> GetLayoutTestTree(Connection &con, const duckdb::string &table_name) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + table_name + " (i INTEGER)"));
	duckdb::shared_ptr<RowGroupSegmentTree> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetRowGroupCollection()->GetRowGroups();
	});
	return result;
}

static duckdb::shared_ptr<const LayoutPatch> MakeEmptyReplacementPatch(row_t start, row_t end, uint64_t task_id) {
	auto patch = make_shared_ptr<LayoutPatch>();
	patch->task_id = hugeint_t(0, task_id);
	patch->range = {start, end};
	patch->sort_order_id = 1;
	patch->run_id = task_id;
	patch->replaced_physical_rows = NumericCast<idx_t>(end - start);
	patch->replacement_physical_rows = 0;
	return patch;
}

TEST_CASE("Table layout history selects layouts by transaction start time", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto tree = GetLayoutTestTree(con, "layout_history_test");

	auto initial = make_shared_ptr<RowGroupLayout>(INITIAL_LAYOUT_VERSION, 0, tree);
	TableLayoutHistory history(initial);
	auto version_one = make_shared_ptr<RowGroupLayout>(
	    1, 10, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {MakeEmptyReplacementPatch(0, 10, 1)});
	history.Publish(version_one);
	auto version_two =
	    make_shared_ptr<RowGroupLayout>(2, 20, tree,
	                                    duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {
	                                        MakeEmptyReplacementPatch(0, 10, 1), MakeEmptyReplacementPatch(10, 20, 2)});
	history.Publish(version_two);

	REQUIRE(history.GetForTransaction(9)->layout_version == 0);
	REQUIRE(history.GetForTransaction(10)->layout_version == 1);
	REQUIRE(history.GetForTransaction(19)->layout_version == 1);
	REQUIRE(history.GetForTransaction(20)->layout_version == 2);
	REQUIRE(history.GetCurrent()->layout_version == 2);
}

TEST_CASE("Table layout history cleanup keeps layouts needed by active transactions", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto tree = GetLayoutTestTree(con, "layout_cleanup_test");

	auto initial = make_shared_ptr<RowGroupLayout>(INITIAL_LAYOUT_VERSION, 0, tree);
	duckdb::weak_ptr<const RowGroupLayout> initial_reference(initial);
	TableLayoutHistory history(initial);
	initial.reset();

	auto version_one = make_shared_ptr<RowGroupLayout>(1, 10, tree);
	duckdb::weak_ptr<const RowGroupLayout> version_one_reference(version_one);
	history.Publish(version_one);
	version_one.reset();
	history.Publish(make_shared_ptr<RowGroupLayout>(2, 20, tree));

	history.Cleanup(9);
	REQUIRE(!initial_reference.expired());
	REQUIRE(!version_one_reference.expired());

	history.Cleanup(10);
	REQUIRE(initial_reference.expired());
	REQUIRE(!version_one_reference.expired());

	history.Cleanup(20);
	REQUIRE(version_one_reference.expired());
}

TEST_CASE("Checkpoint tree installation preserves the current layout version", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto tree = GetLayoutTestTree(con, "layout_checkpoint_source");
	auto checkpoint_tree = GetLayoutTestTree(con, "layout_checkpoint_target");

	TableLayoutHistory history(make_shared_ptr<RowGroupLayout>(INITIAL_LAYOUT_VERSION, 0, tree));
	history.Publish(make_shared_ptr<RowGroupLayout>(
	    1, 10, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {MakeEmptyReplacementPatch(0, 10, 1)}));
	auto pinned_layout = history.GetCurrent();
	history.InstallCheckpointTree(checkpoint_tree);

	auto installed_layout = history.GetCurrent();
	REQUIRE(installed_layout.get() != pinned_layout.get());
	REQUIRE(installed_layout->layout_version == pinned_layout->layout_version);
	REQUIRE(installed_layout->visible_from == pinned_layout->visible_from);
	REQUIRE(installed_layout->base_tree.get() == checkpoint_tree.get());
	REQUIRE(installed_layout->patches.empty());
	REQUIRE(pinned_layout->patches.size() == 1);
	REQUIRE(history.GetForTransaction(10).get() == installed_layout.get());
}

TEST_CASE("Row group layouts reject invalid patch sequences", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto tree = GetLayoutTestTree(con, "layout_validation_test");

	duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> overlapping_patches;
	overlapping_patches.push_back(MakeEmptyReplacementPatch(0, 10, 1));
	overlapping_patches.push_back(MakeEmptyReplacementPatch(9, 20, 2));
	REQUIRE_THROWS_AS(make_shared_ptr<RowGroupLayout>(1, 10, tree, std::move(overlapping_patches)), InternalException);

	duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> excessive_patches;
	for (idx_t patch_idx = 0; patch_idx <= MAX_LAYOUT_PATCHES_PER_CHECKPOINT; patch_idx++) {
		auto start = NumericCast<row_t>(patch_idx * 10);
		excessive_patches.push_back(MakeEmptyReplacementPatch(start, start + 10, patch_idx + 1));
	}
	REQUIRE_THROWS_AS(make_shared_ptr<RowGroupLayout>(1, 10, tree, std::move(excessive_patches)), InternalException);

	TableLayoutHistory history(make_shared_ptr<RowGroupLayout>(INITIAL_LAYOUT_VERSION, 0, tree));
	REQUIRE_THROWS_AS(history.Publish(make_shared_ptr<RowGroupLayout>(2, 10, tree)), InternalException);
	REQUIRE_THROWS_AS(history.Publish(make_shared_ptr<RowGroupLayout>(1, 0, tree)), InternalException);
}
