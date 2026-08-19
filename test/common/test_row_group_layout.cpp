#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<RowGroupCollection> GetLayoutTestCollection(Connection &con,
                                                                      const duckdb::string &table_name) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + table_name + " (i INTEGER)"));
	duckdb::shared_ptr<RowGroupCollection> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetRowGroupCollection();
	});
	return result;
}

static duckdb::shared_ptr<RowGroupSegmentTree> GetLayoutTestTree(Connection &con, const duckdb::string &table_name) {
	return GetLayoutTestCollection(con, table_name)->GetRowGroups();
}

static duckdb::shared_ptr<RowGroupSegmentTree> MakeLayoutTestTree(RowGroupCollection &collection,
                                                                  const duckdb::vector<idx_t> &counts) {
	auto tree = make_shared_ptr<RowGroupSegmentTree>(collection, 0);
	idx_t row_start = 0;
	for (auto count : counts) {
		tree->AppendSegment(make_shared_ptr<RowGroup>(collection, count), row_start);
		row_start += count;
	}
	return tree;
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

static duckdb::shared_ptr<const LayoutPatch>
MakeReplacementPatch(row_t start, row_t end, uint64_t task_id,
                     duckdb::vector<duckdb::shared_ptr<RowGroup>> replacement_groups) {
	auto patch = make_shared_ptr<LayoutPatch>();
	patch->task_id = hugeint_t(0, task_id);
	patch->range = {start, end};
	patch->sort_order_id = 1;
	patch->run_id = task_id;
	patch->replaced_physical_rows = NumericCast<idx_t>(end - start);
	for (auto &row_group : replacement_groups) {
		patch->replacement_physical_rows += row_group->count.load();
	}
	patch->replacement_groups = std::move(replacement_groups);
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

TEST_CASE("Layout row group cursor merges patches and preserves row ID gaps", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_cursor_test");
	auto tree = MakeLayoutTestTree(*collection, {10, 10, 10});
	auto first_replacement = make_shared_ptr<RowGroup>(*collection, 6);
	auto second_replacement = make_shared_ptr<RowGroup>(*collection, 3);
	auto patch = MakeReplacementPatch(10, 20, 1, {first_replacement, second_replacement});
	auto layout =
	    make_shared_ptr<RowGroupLayout>(1, 10, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {patch});
	RowGroupCollectionSnapshot snapshot(layout);
	LayoutRowGroupCursor cursor(snapshot);

	duckdb::vector<LayoutRowGroupEntry> entries;
	LayoutRowGroupEntry entry;
	while (cursor.Next(entry)) {
		entries.push_back(entry);
	}
	REQUIRE(entries.size() == 4);
	REQUIRE(entries[0].row_start == 0);
	REQUIRE(entries[0].GetRowEnd() == 10);
	REQUIRE(entries[0].layout_index == 0);
	REQUIRE(entries[1].row_group.get() == first_replacement.get());
	REQUIRE(entries[1].row_start == 10);
	REQUIRE(entries[1].GetRowEnd() == 16);
	REQUIRE(entries[1].layout_index == 1);
	REQUIRE(entries[2].row_group.get() == second_replacement.get());
	REQUIRE(entries[2].row_start == 16);
	REQUIRE(entries[2].GetRowEnd() == 19);
	REQUIRE(entries[2].layout_index == 2);
	REQUIRE(entries[3].row_start == 20);
	REQUIRE(entries[3].GetRowEnd() == 30);
	REQUIRE(entries[3].layout_index == 3);

	REQUIRE(snapshot.Lookup(15, entry));
	REQUIRE(entry.row_group.get() == first_replacement.get());
	REQUIRE(snapshot.Lookup(16, entry));
	REQUIRE(entry.row_group.get() == second_replacement.get());
	REQUIRE(!snapshot.Lookup(19, entry));
	REQUIRE(snapshot.Lookup(20, entry));
	REQUIRE(entry.row_group.get() == entries[3].row_group.get());
	REQUIRE(!snapshot.Lookup(30, entry));

	LayoutRowGroupCursor range_cursor(snapshot, RowGroupRange {15, 21});
	entries.clear();
	while (range_cursor.Next(entry)) {
		entries.push_back(entry);
	}
	REQUIRE(entries.size() == 3);
	REQUIRE(entries[0].row_start == 10);
	REQUIRE(entries[1].row_start == 16);
	REQUIRE(entries[2].row_start == 20);

	LayoutRowGroupCursor empty_range_cursor(snapshot, RowGroupRange {20, 20});
	REQUIRE(!empty_range_cursor.Next(entry));
}

TEST_CASE("Layout row group cursor supports adjacent and empty patches", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_adjacent_patch_test");
	auto tree = MakeLayoutTestTree(*collection, {10, 10, 10, 10});
	auto replacement = make_shared_ptr<RowGroup>(*collection, 10);
	auto layout = make_shared_ptr<RowGroupLayout>(
	    1, 10, tree,
	    duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {MakeEmptyReplacementPatch(0, 10, 1),
	                                                           MakeReplacementPatch(10, 20, 2, {replacement})});
	LayoutRowGroupCursor cursor {RowGroupCollectionSnapshot(layout)};

	LayoutRowGroupEntry entry;
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_group.get() == replacement.get());
	REQUIRE(entry.row_start == 10);
	REQUIRE(entry.layout_index == 0);
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_start == 20);
	REQUIRE(entry.layout_index == 1);
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_start == 30);
	REQUIRE(entry.layout_index == 2);
	REQUIRE(!cursor.Next(entry));

	RowGroupCollectionSnapshot base_snapshot(tree);
	REQUIRE(base_snapshot.Lookup(10, entry));
	REQUIRE(entry.row_start == 10);
	REQUIRE(entry.layout_index == 1);
}

TEST_CASE("Layout row group cursor rejects patches inside base row groups", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_cursor_alignment_test");
	auto tree = MakeLayoutTestTree(*collection, {10, 10});
	auto replacement = make_shared_ptr<RowGroup>(*collection, 10);
	auto layout = make_shared_ptr<RowGroupLayout>(
	    1, 10, tree,
	    duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {MakeReplacementPatch(5, 15, 1, {replacement})});
	LayoutRowGroupCursor cursor {RowGroupCollectionSnapshot(layout)};
	LayoutRowGroupEntry entry;
	REQUIRE_THROWS_AS(cursor.Next(entry), InternalException);
}

TEST_CASE("Layout row group cursor accepts row ID gaps between replaced groups", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_cursor_base_gap_test");
	auto tree = make_shared_ptr<RowGroupSegmentTree>(*collection, 0);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 5), 0);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 10), 10);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 10), 20);

	auto replacement = make_shared_ptr<RowGroup>(*collection, 12);
	auto patch = make_shared_ptr<LayoutPatch>();
	patch->task_id = hugeint_t(0, 1);
	patch->range = {0, 20};
	patch->sort_order_id = 1;
	patch->run_id = 1;
	patch->replaced_physical_rows = 15;
	patch->replacement_physical_rows = 12;
	patch->replacement_groups.push_back(replacement);
	auto layout =
	    make_shared_ptr<RowGroupLayout>(1, 10, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {patch});
	RowGroupCollectionSnapshot snapshot(layout);
	LayoutRowGroupCursor cursor(snapshot);

	LayoutRowGroupEntry entry;
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_group.get() == replacement.get());
	REQUIRE(entry.row_start == 0);
	REQUIRE(entry.GetRowEnd() == 12);
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_start == 20);
	REQUIRE(!cursor.Next(entry));
	REQUIRE(!snapshot.Lookup(12, entry));
	REQUIRE(snapshot.Lookup(20, entry));
	REQUIRE(entry.layout_index == DConstants::INVALID_INDEX);
}
