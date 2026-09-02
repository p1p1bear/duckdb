#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

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

static void AppendLayoutTestValue(RowGroupCollection &collection, int32_t value,
                                  const AppendOrganization &organization) {
	TableAppendState append_state;
	collection.InitializeAppend(TransactionData::Committed(), append_state, organization);
	DataChunk chunk;
	chunk.Initialize(collection.GetAllocator(), {LogicalType::INTEGER});
	chunk.data[0].Append(Value::INTEGER(value));
	chunk.SetChildCardinality(1);
	collection.Append(chunk, append_state);
	collection.FinalizeAppend(TransactionData::Committed(), append_state);
}

static duckdb::unique_ptr<OptimisticWriteCollection> MakeOptimisticLayoutTestCollection(Connection &con,
                                                                                        const string &table_name) {
	duckdb::unique_ptr<OptimisticWriteCollection> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		OptimisticDataWriter writer(*con.context, entry.GetStorage());
		result = writer.CreateCollection(entry.GetStorage(), entry.GetTypes());
		result->collection->InitializeEmpty();
	});
	return result;
}

static void AppendOptimisticLayoutTestValues(OptimisticWriteCollection &collection,
                                             const duckdb::vector<int32_t> &values,
                                             const AppendOrganization &organization) {
	TableAppendState append_state;
	collection.InitializeAppend(TransactionData::Committed(), append_state, organization);
	DataChunk chunk;
	chunk.Initialize(collection.collection->GetAllocator(), {LogicalType::INTEGER});
	for (auto value : values) {
		chunk.data[0].Append(Value::INTEGER(value));
	}
	chunk.SetChildCardinality(values.size());
	collection.Append(chunk, append_state);
	collection.FinalizeAppend(TransactionData::Committed(), append_state);
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
	auto collection = GetLayoutTestCollection(con, "layout_history_test");
	auto tree = MakeLayoutTestTree(*collection, {10, 10});

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

TEST_CASE("Table layout history reverts only the current publication", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto tree = GetLayoutTestTree(con, "layout_revert_test");

	auto initial = make_shared_ptr<RowGroupLayout>(INITIAL_LAYOUT_VERSION, 0, tree);
	TableLayoutHistory history(initial);
	auto version_one = make_shared_ptr<RowGroupLayout>(1, 10, tree);
	auto version_two = make_shared_ptr<RowGroupLayout>(2, 20, tree);
	history.Publish(version_one);
	history.Publish(version_two);

	REQUIRE_THROWS_AS(history.RevertPublished(version_one, initial), InternalException);
	REQUIRE(history.GetCurrent().get() == version_two.get());
	history.RevertPublished(version_two, version_one);
	REQUIRE(history.GetCurrent().get() == version_one.get());
	REQUIRE(history.GetForTransaction(9).get() == initial.get());
	REQUIRE(history.GetForTransaction(20).get() == version_one.get());
	REQUIRE_THROWS_AS(history.RevertPublished(version_two, version_one), InternalException);
}

TEST_CASE("Row group append organization enforces run boundaries", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "append_organization_test");

	TableAppendState uninitialized;
	DataChunk chunk;
	chunk.Initialize(collection->GetAllocator(), {LogicalType::INTEGER});
	chunk.data[0].Append(Value::INTEGER(0));
	chunk.SetChildCardinality(1);
	REQUIRE_THROWS_AS(collection->Append(chunk, uninitialized), InternalException);

	TableAppendState invalid;
	REQUIRE_THROWS_AS(collection->InitializeAppend(invalid, AppendOrganization::Sorted(1, 0)), InternalException);

	AppendLayoutTestValue(*collection, 1, AppendOrganization::Unsorted());
	AppendLayoutTestValue(*collection, 2, AppendOrganization::Unsorted());
	REQUIRE(collection->GetRowGroupCount() == 1);
	auto unsorted = collection->GetRowGroup(0);
	REQUIRE(unsorted);
	REQUIRE(unsorted->count == 2);
	REQUIRE(unsorted->GetSortMetadata() == RowGroupSortMetadata());
	REQUIRE(!unsorted->IsSealed());

	AppendLayoutTestValue(*collection, 3, AppendOrganization::Sorted(1, 1));
	REQUIRE(collection->GetRowGroupCount() == 2);
	auto first_run = collection->GetRowGroup(1);
	REQUIRE(first_run);
	REQUIRE(first_run->GetSortMetadata() == RowGroupSortMetadata {1, 1});
	REQUIRE(first_run->IsSealed());

	AppendLayoutTestValue(*collection, 4, AppendOrganization::Sorted(1, 2));
	AppendLayoutTestValue(*collection, 5, AppendOrganization::Unsorted());
	REQUIRE(collection->GetRowGroupCount() == 4);
	REQUIRE(collection->GetRowGroup(2)->GetSortMetadata() == RowGroupSortMetadata {1, 2});
	REQUIRE(collection->GetRowGroup(2)->IsSealed());
	REQUIRE(collection->GetRowGroup(3)->GetSortMetadata() == RowGroupSortMetadata());

	collection->GetRowGroup(3)->SetSortMetadata({}, true);
	AppendLayoutTestValue(*collection, 6, AppendOrganization::Unsorted());
	REQUIRE(collection->GetRowGroupCount() == 5);
	REQUIRE(collection->GetRowGroup(4)->count == 1);
}

TEST_CASE("Optimistic append spans survive ownership transfer and unsorted downgrade", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE optimistic_span_test (i INTEGER)"));

	auto target = MakeOptimisticLayoutTestCollection(con, "optimistic_span_test");
	auto first_source = MakeOptimisticLayoutTestCollection(con, "optimistic_span_test");
	auto second_source = MakeOptimisticLayoutTestCollection(con, "optimistic_span_test");
	AppendOptimisticLayoutTestValues(*target, {1}, AppendOrganization::Unsorted());
	AppendOptimisticLayoutTestValues(*first_source, {2, 3}, AppendOrganization::Sorted(1, 1));
	AppendOptimisticLayoutTestValues(*first_source, {4}, AppendOrganization::Sorted(1, 2));
	AppendOptimisticLayoutTestValues(*second_source, {5, 6}, AppendOrganization::Unsorted());

	target->MergeStorage(*first_source);
	target->MergeStorage(*second_source);
	target->VerifyAppendSpans(6);
	REQUIRE(target->append_spans.size() == 4);
	REQUIRE(target->append_spans[0].collection_offset == 0);
	REQUIRE(target->append_spans[1].collection_offset == 1);
	REQUIRE(target->append_spans[2].collection_offset == 3);
	REQUIRE(target->append_spans[3].collection_offset == 4);
	REQUIRE(first_source->append_spans.empty());
	REQUIRE(second_source->append_spans.empty());

	REQUIRE(target->collection->GetRowGroupCount() == 4);
	REQUIRE(target->collection->GetRowGroup(1)->GetSortMetadata() == RowGroupSortMetadata {1, 1});
	REQUIRE(target->collection->GetRowGroup(2)->GetSortMetadata() == RowGroupSortMetadata {1, 2});

	AppendOrganizationSpanCursor cursor(target->append_spans);
	REQUIRE(cursor.Remaining() == 1);
	REQUIRE(cursor.Advance(1));
	REQUIRE(cursor.Remaining() == 2);
	REQUIRE(!cursor.Advance(1));
	REQUIRE(cursor.Advance(1));
	REQUIRE(cursor.Advance(1));
	REQUIRE(cursor.Advance(2));
	cursor.VerifyFinished();

	target->ForceUnsorted(5);
	target->VerifyAppendSpans(5);
	REQUIRE(target->append_spans.size() == 1);
	REQUIRE(target->append_spans[0].organization == AppendOrganization::Unsorted());
	for (idx_t row_group_idx = 0; row_group_idx < target->collection->GetRowGroupCount(); row_group_idx++) {
		auto row_group = target->collection->GetRowGroup(NumericCast<int64_t>(row_group_idx));
		REQUIRE(row_group);
		REQUIRE(!row_group->GetSortMetadata().IsSorted());
		REQUIRE(!row_group->IsSealed());
	}

	AppendOptimisticLayoutTestValues(*target, {7}, AppendOrganization::Unsorted());
	target->VerifyAppendSpans(6);
	REQUIRE(target->append_spans.size() == 1);
	REQUIRE(target->append_spans[0].physical_count == 6);
}

TEST_CASE("Transaction-local deletes make append organization sticky unsorted", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE local_span_delete_test (i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	con.context->RunFunctionInTransaction([&]() {
		auto &entry =
		    Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("local_span_delete_test")));
		auto &table = entry.GetStorage();
		MetaTransaction::Get(*con.context).ModifyDatabase(table.db, DatabaseModificationType());
		auto binder = Binder::CreateBinder(*con.context);
		auto bound_constraints = binder->BindConstraints(entry);

		DataChunk first_chunk;
		first_chunk.Initialize(*con.context, {LogicalType::INTEGER});
		first_chunk.data[0].Append(Value::INTEGER(1));
		first_chunk.data[0].Append(Value::INTEGER(2));
		first_chunk.SetChildCardinality(2);
		table.LocalAppend(entry, *con.context, first_chunk, bound_constraints, false,
		                  AppendOrganization::Sorted(1, 1));

		auto &local_storage = LocalStorage::Get(*con.context, table.db);
		auto local_table = local_storage.GetStorage(table);
		REQUIRE(local_table);
		REQUIRE(local_table->GetPrimaryCollection().append_spans[0].organization == AppendOrganization::Sorted(1, 1));

		Vector row_ids(LogicalType::ROW_TYPE);
		row_ids.Append(Value::BIGINT(MAX_ROW_ID));
		REQUIRE(local_storage.Delete(table, entry, row_ids, 1) == 1);
		REQUIRE(local_table->force_unsorted_on_commit);
		local_table->GetPrimaryCollection().VerifyAppendSpans(1);
		REQUIRE(local_table->GetPrimaryCollection().append_spans[0].organization == AppendOrganization::Unsorted());

		DataChunk second_chunk;
		second_chunk.Initialize(*con.context, {LogicalType::INTEGER});
		second_chunk.data[0].Append(Value::INTEGER(3));
		second_chunk.SetChildCardinality(1);
		table.LocalAppend(entry, *con.context, second_chunk, bound_constraints, false,
		                  AppendOrganization::Sorted(1, 2));
		local_table->GetPrimaryCollection().VerifyAppendSpans(2);
		REQUIRE(local_table->GetPrimaryCollection().append_spans.size() == 1);
		REQUIRE(local_table->GetPrimaryCollection().append_spans[0].organization == AppendOrganization::Unsorted());
	});

	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	auto result = con.Query("SELECT i FROM local_span_delete_test ORDER BY i");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));

	duckdb::shared_ptr<RowGroupCollection> collection;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry =
		    Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("local_span_delete_test")));
		collection = entry.GetStorage().GetRowGroupCollection();
	});
	REQUIRE(collection->GetRowGroupCount() == 1);
	REQUIRE(!collection->GetRowGroup(0)->GetSortMetadata().IsSorted());
	REQUIRE(!collection->GetRowGroup(0)->IsSealed());
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
	auto collection = GetLayoutTestCollection(con, "layout_checkpoint_source");
	auto tree = MakeLayoutTestTree(*collection, {10});
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
	REQUIRE_THROWS_AS(history.InstallCheckpointTree(tree, pinned_layout), InternalException);
	REQUIRE_THROWS_AS(history.Publish(make_shared_ptr<RowGroupLayout>(2, 20, tree)), InternalException);
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
	REQUIRE_THROWS_AS(make_shared_ptr<RowGroupLayout>(1, 10, tree,
	                                                  duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {
	                                                      MakeReplacementPatch(5, 15, 1, {replacement})}),
	                  InternalException);
}

TEST_CASE("Layout lookup and range cursors seek within large patches", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_large_patch_seek_test");
	duckdb::vector<idx_t> base_counts(256, 1);
	auto tree = MakeLayoutTestTree(*collection, base_counts);
	duckdb::vector<duckdb::shared_ptr<RowGroup>> replacements;
	for (idx_t index = 0; index < 200; index++) {
		replacements.push_back(make_shared_ptr<RowGroup>(*collection, 1));
	}
	auto patch = MakeReplacementPatch(10, 210, 1, std::move(replacements));
	auto layout = make_shared_ptr<RowGroupLayout>(
	    1, 10, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {std::move(patch)});
	RowGroupCollectionSnapshot snapshot(layout);

	LayoutRowGroupEntry entry;
	REQUIRE(snapshot.Lookup(209, entry));
	REQUIRE(entry.row_start == 209);
	REQUIRE(!snapshot.Lookup(256, entry));

	LayoutRowGroupCursor cursor(snapshot, RowGroupRange {205, 211});
	for (row_t expected = 205; expected < 211; expected++) {
		REQUIRE(cursor.Next(entry));
		REQUIRE(entry.row_start == expected);
		REQUIRE(entry.layout_index == NumericCast<idx_t>(expected));
	}
	REQUIRE(!cursor.Next(entry));
}

TEST_CASE("Layout range cursors seek across base row ID gaps", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_gap_seek_test");
	auto tree = make_shared_ptr<RowGroupSegmentTree>(*collection, 0);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 5), 0);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 5), 10);
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 5), 20);
	auto layout = make_shared_ptr<RowGroupLayout>(1, 10, tree);

	LayoutRowGroupCursor cursor(RowGroupCollectionSnapshot(layout), RowGroupRange {6, 11});
	LayoutRowGroupEntry entry;
	REQUIRE(cursor.Next(entry));
	REQUIRE(entry.row_start == 10);
	REQUIRE(entry.layout_index == 1);
	REQUIRE(!cursor.Next(entry));
}

TEST_CASE("Layout range cursors preserve indexes across multiple patches", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "layout_multi_patch_seek_test");
	auto tree = MakeLayoutTestTree(*collection, {10, 10, 10, 10, 10, 10});
	auto first_patch = MakeReplacementPatch(
	    10, 20, 1, {make_shared_ptr<RowGroup>(*collection, 5), make_shared_ptr<RowGroup>(*collection, 5)});
	auto second_patch = MakeReplacementPatch(30, 50, 2, {make_shared_ptr<RowGroup>(*collection, 15)});
	auto layout = make_shared_ptr<RowGroupLayout>(
	    1, 10, tree,
	    duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {std::move(first_patch), std::move(second_patch)});
	auto snapshot = RowGroupCollectionSnapshot(layout);

	LayoutRowGroupEntry entry;
	LayoutRowGroupCursor replacement_cursor(snapshot, RowGroupRange {40, 41});
	REQUIRE(replacement_cursor.Next(entry));
	REQUIRE(entry.row_start == 30);
	REQUIRE(entry.layout_index == 4);
	REQUIRE(!replacement_cursor.Next(entry));

	LayoutRowGroupCursor replacement_gap_cursor(snapshot, RowGroupRange {47, 51});
	REQUIRE(replacement_gap_cursor.Next(entry));
	REQUIRE(entry.row_start == 50);
	REQUIRE(entry.layout_index == 5);
	REQUIRE(!replacement_gap_cursor.Next(entry));

	LayoutRowGroupCursor trailing_base_cursor(snapshot, RowGroupRange {55, 56});
	REQUIRE(trailing_base_cursor.Next(entry));
	REQUIRE(entry.row_start == 50);
	REQUIRE(entry.layout_index == 5);
	REQUIRE(!trailing_base_cursor.Next(entry));
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

TEST_CASE("Row group collection selects and installs versioned layouts", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	auto collection = GetLayoutTestCollection(con, "collection_layout_history_test");
	auto tree = collection->GetRowGroups();
	tree->AppendSegment(make_shared_ptr<RowGroup>(*collection, 10), 0);

	auto base_snapshot = collection->GetSnapshot(TransactionData(100, 5));
	REQUIRE(base_snapshot.kind == RowGroupCollectionSnapshot::Kind::BASE_TREE);

	collection->InitializeLayoutHistory(INITIAL_LAYOUT_VERSION);
	collection->InitializeLayoutHistory(INITIAL_LAYOUT_VERSION);
	REQUIRE(collection->HasLayoutHistory());
	REQUIRE_THROWS_AS(collection->InitializeLayoutHistory(1), InternalException);

	auto replacement = make_shared_ptr<RowGroup>(*collection, 8);
	auto patch = MakeReplacementPatch(0, 10, 1, {replacement});
	auto published_layout = collection->BuildPatchedLayout(10, patch);
	REQUIRE(published_layout->layout_version == 1);
	REQUIRE(published_layout->patches.size() == 1);
	collection->PublishLayout(published_layout);

	auto old_snapshot = collection->GetSnapshot(TransactionData(101, 9));
	auto new_snapshot = collection->GetSnapshot(TransactionData(102, 10));
	auto committed_snapshot = collection->GetSnapshot(TransactionData::Committed());
	REQUIRE(old_snapshot.kind == RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT);
	REQUIRE(old_snapshot.layout->layout_version == 0);
	REQUIRE(new_snapshot.layout.get() == published_layout.get());
	REQUIRE(committed_snapshot.layout.get() == published_layout.get());

	auto overlapping_patch = MakeEmptyReplacementPatch(0, 10, 2);
	REQUIRE_THROWS_AS(collection->BuildPatchedLayout(20, overlapping_patch), InternalException);

	auto checkpoint_tree = MakeLayoutTestTree(*collection, {8});
	collection->InstallCheckpointTree(checkpoint_tree);
	auto checkpoint_layout = collection->GetCurrentLayout();
	REQUIRE(checkpoint_layout->layout_version == 1);
	REQUIRE(checkpoint_layout->visible_from == 10);
	REQUIRE(checkpoint_layout->patches.empty());
	REQUIRE(checkpoint_layout->base_tree.get() == checkpoint_tree.get());
	REQUIRE(collection->GetRowGroups().get() == checkpoint_tree.get());
	REQUIRE(collection->GetSnapshot(TransactionData(103, 9)).layout->layout_version == 0);
}

TEST_CASE("Checkpoint materializes the current row group layout", "[storage][row_group_layout]") {
	auto path = TestCreatePath("layout_checkpoint_materialization.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS layout_checkpoint (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE layout_checkpoint"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT layout_checkpoint"));

		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			auto tree = collection->GetRowGroups();
			REQUIRE(tree->GetSegmentCount() == 2);
			auto first = tree->GetRootSegment();
			REQUIRE(first);
			REQUIRE(first->GetRowStart() == 0);
			collection->PublishLayout(collection->BuildPatchedLayout(
			    1, MakeEmptyReplacementPatch(0, NumericCast<row_t>(first->GetCount()), 7)));
			entry.GetStorage().GetDataTableInfo()->GetSortStorage().current_layout_version.store(1);
		});

		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (5000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT layout_checkpoint"));
		auto result = con.Query("SELECT count(*), min(i), max(i) FROM tbl");
		REQUIRE_NO_FAIL(*result);
		CHECK_COLUMN(result, 0, {Value::BIGINT(2049)});
		CHECK_COLUMN(result, 1, {Value::INTEGER(2048)});
		CHECK_COLUMN(result, 2, {Value::INTEGER(5000)});
	}
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS layout_checkpoint"));
		REQUIRE_NO_FAIL(con.Query("USE layout_checkpoint"));
		auto result = con.Query("SELECT count(*), min(i), max(i) FROM tbl");
		REQUIRE_NO_FAIL(*result);
		CHECK_COLUMN(result, 0, {Value::BIGINT(2049)});
		CHECK_COLUMN(result, 1, {Value::INTEGER(2048)});
		CHECK_COLUMN(result, 2, {Value::INTEGER(5000)});

		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			REQUIRE(collection->GetTotalRows() == 2049);
			REQUIRE(collection->GetNextRowId() == 4097);
			REQUIRE(collection->GetCurrentLayout()->layout_version == 1);
			REQUIRE(collection->GetCurrentLayout()->patches.empty());
		});
	}
	DeleteDatabase(path);
}

TEST_CASE("All row group scan entry points honor transaction layouts", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE layout_scan_test (i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO layout_scan_test VALUES (1), (2), (3)"));

	duckdb::shared_ptr<RowGroupCollection> collection;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("layout_scan_test")));
		collection = entry.GetStorage().GetRowGroupCollection();
	});
	auto tree = collection->GetRowGroups();
	auto root = tree->GetRootSegment();
	REQUIRE(root);
	auto row_count = root->GetNode().count.load();
	REQUIRE(row_count == 3);

	collection->InitializeLayoutHistory(INITIAL_LAYOUT_VERSION);
	auto replacement_patch = MakeReplacementPatch(0, NumericCast<row_t>(row_count), 1, {root->ReferenceNode()});
	collection->PublishLayout(collection->BuildPatchedLayout(10, std::move(replacement_patch)));
	auto empty_patch = MakeEmptyReplacementPatch(0, NumericCast<row_t>(row_count), 2);
	collection->PublishLayout(make_shared_ptr<RowGroupLayout>(
	    2, 20, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {std::move(empty_patch)}));
	duckdb::vector<StorageIndex> column_ids {StorageIndex(0)};
	REQUIRE(collection->CanFetch(TransactionData(90, 9), 0));
	REQUIRE(collection->CanFetch(TransactionData(91, 10), 0));
	REQUIRE(!collection->CanFetch(TransactionData(92, 20), 0));

	Vector row_ids(LogicalType::ROW_TYPE);
	row_ids.Append(Value::BIGINT(0));
	row_ids.Append(Value::BIGINT(1));
	row_ids.Append(Value::BIGINT(2));
	DataChunk fetched;
	fetched.Initialize(collection->GetAllocator(), {LogicalType::INTEGER});
	ColumnFetchState fetch_state;
	collection->Fetch(TransactionData(93, 10), fetched, column_ids, row_ids, 3, fetch_state);
	REQUIRE(fetched.size() == 3);
	REQUIRE(fetched.GetValue(0, 0) == Value::INTEGER(1));
	REQUIRE(fetched.GetValue(0, 1) == Value::INTEGER(2));
	REQUIRE(fetched.GetValue(0, 2) == Value::INTEGER(3));
	fetched.Reset();
	collection->Fetch(TransactionData(94, 20), fetched, column_ids, row_ids, 3, fetch_state);
	REQUIRE(fetched.size() == 0);
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("layout_scan_test")));
		row_t row_id = 0;
		REQUIRE_THROWS_AS(collection->Delete(TransactionData::Committed(), entry, &row_id, 1), InternalException);

		DataChunk updates;
		updates.Initialize(collection->GetAllocator(), {LogicalType::INTEGER});
		updates.data[0].Append(Value::INTEGER(42));
		updates.SetChildCardinality(1);
		duckdb::vector<PhysicalIndex> update_columns {PhysicalIndex(0)};
		REQUIRE_THROWS_AS(collection->Update(TransactionData::Committed(), entry, &row_id, update_columns, updates),
		                  InternalException);
	});

	auto scan_rows = [&](TableScanState &scan_state) {
		DataChunk result;
		result.Initialize(collection->GetAllocator(), {LogicalType::INTEGER});
		idx_t count = 0;
		while (true) {
			result.Reset();
			if (!scan_state.table_state.Scan(result, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
				break;
			}
			count += result.size();
		}
		return count;
	};

	TableScanState old_scan;
	old_scan.Initialize(column_ids, nullptr);
	collection->InitializeScan(TransactionData(100, 9), QueryContext(), old_scan.table_state, column_ids, nullptr);
	REQUIRE(old_scan.table_state.row_group);
	REQUIRE(scan_rows(old_scan) == 3);

	TableScanState replacement_scan;
	replacement_scan.Initialize(column_ids, nullptr);
	collection->InitializeScan(TransactionData(101, 10), QueryContext(), replacement_scan.table_state, column_ids,
	                           nullptr);
	REQUIRE(replacement_scan.table_state.row_group);
	REQUIRE(scan_rows(replacement_scan) == 3);

	TableScanState current_scan;
	current_scan.Initialize(column_ids, nullptr);
	collection->InitializeScan(TransactionData(102, 20), QueryContext(), current_scan.table_state, column_ids, nullptr);
	REQUIRE(!current_scan.table_state.row_group);

	TableScanState old_range_scan;
	old_range_scan.Initialize(column_ids, nullptr);
	collection->InitializeScanWithOffset(TransactionData(103, 9), QueryContext(), old_range_scan.table_state,
	                                     column_ids, 0, 2);
	REQUIRE(old_range_scan.table_state.row_group);

	TableScanState current_range_scan;
	current_range_scan.Initialize(column_ids, nullptr);
	collection->InitializeScanWithOffset(TransactionData(104, 20), QueryContext(), current_range_scan.table_state,
	                                     column_ids, 0, 2);
	REQUIRE(!current_range_scan.table_state.row_group);

	ParallelCollectionScanState old_parallel_scan;
	collection->InitializeParallelScan(TransactionData(105, 9), old_parallel_scan);
	REQUIRE(old_parallel_scan.current_layout_row_group);
	ParallelCollectionScanState replacement_parallel_scan;
	collection->InitializeParallelScan(TransactionData(106, 10), replacement_parallel_scan);
	REQUIRE(replacement_parallel_scan.current_layout_row_group);
	TableScanState parallel_local_scan;
	parallel_local_scan.Initialize(column_ids, nullptr);
	idx_t parallel_count = 0;
	while (collection->NextParallelScan(*con.context, replacement_parallel_scan, parallel_local_scan.table_state)) {
		parallel_count += scan_rows(parallel_local_scan);
	}
	REQUIRE(parallel_count == 3);
	ParallelCollectionScanState current_parallel_scan;
	collection->InitializeParallelScan(TransactionData(107, 20), current_parallel_scan);
	REQUIRE(!current_parallel_scan.current_layout_row_group);

	TableScanState reordered_scan;
	reordered_scan.Initialize(column_ids, nullptr);
	RowGroupOrderOptions order_options(StorageIndex(0), OrderByStatistics::MIN, OrderType::ASCENDING,
	                                   OrderByNullType::NULLS_LAST, OrderByColumnType::NUMERIC);
	reordered_scan.table_state.reorderer = make_uniq<RowGroupReorderer>(order_options, TransactionData(108, 20));
	collection->InitializeScan(TransactionData(108, 20), QueryContext(), reordered_scan.table_state, column_ids,
	                           nullptr);
	REQUIRE(!reordered_scan.table_state.row_group);
}

TEST_CASE("Physical schema transforms consume the current row group layout", "[storage][row_group_layout]") {
	auto path = TestCreatePath("layout_schema_alter.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS layout_alter (ROW_GROUP_SIZE 2048)"));
	REQUIRE_NO_FAIL(con.Query("USE layout_alter"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER, (i * 10)::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT layout_alter"));

	duckdb::shared_ptr<RowGroupCollection> collection;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		collection = entry.GetStorage().GetRowGroupCollection();
	});
	auto tree = collection->GetRowGroups();
	REQUIRE(tree->GetSegmentCount() == 2);
	auto first = tree->GetRootSegment();
	REQUIRE(first);
	REQUIRE(first->GetRowStart() == 0);
	REQUIRE(first->GetCount() == 2048);
	auto remaining_rows = 4096 - first->GetCount();

	collection->InitializeLayoutHistory(INITIAL_LAYOUT_VERSION);
	collection->PublishLayout(collection->BuildPatchedLayout(10, MakeEmptyReplacementPatch(0, first->GetCount(), 1)));
	auto source_row_group = collection->GetRowGroup(1);
	REQUIRE(source_row_group);
	auto source_i_ownership = source_row_group->GetColumnDropOwnershipBundle(0);
	auto source_j_ownership = source_row_group->GetColumnDropOwnershipBundle(1);

	auto scan_column = [&](RowGroupCollection &source, StorageIndex column_id, const LogicalType &type) {
		duckdb::vector<Value> result;
		duckdb::vector<StorageIndex> column_ids {column_id};
		TableScanState scan_state;
		scan_state.Initialize(column_ids, nullptr);
		source.InitializeScan(TransactionData::Committed(), QueryContext(), scan_state.table_state, column_ids,
		                      nullptr);
		DataChunk chunk;
		chunk.Initialize(source.GetAllocator(), {type});
		while (true) {
			chunk.Reset();
			if (!scan_state.table_state.Scan(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
				break;
			}
			for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
				result.push_back(chunk.GetValue(0, row_idx));
			}
		}
		return result;
	};

	BoundConstantExpression default_value(Value::INTEGER(99));
	ExpressionExecutor default_executor(*con.context);
	default_executor.AddExpression(default_value);
	ColumnDefinition new_column("k", LogicalType::INTEGER);
	auto added = collection->AddColumn(*con.context, new_column, default_executor);
	REQUIRE(added->GetTotalRows() == remaining_rows);
	REQUIRE(added->GetNextRowId() == 4096);
	REQUIRE(added->HasLayoutHistory());
	REQUIRE(added->GetCurrentLayout()->layout_version == 1);
	REQUIRE(added->GetCurrentLayout()->patches.empty());
	auto added_row_group = added->GetRowGroup(0);
	REQUIRE(added_row_group);
	REQUIRE(added_row_group->GetColumnDropOwnershipBundle(0) == source_i_ownership);
	REQUIRE(added_row_group->GetColumnDropOwnershipBundle(1) == source_j_ownership);
	REQUIRE(added_row_group->GetColumnDropOwnershipBundle(2) != source_i_ownership);
	REQUIRE(added_row_group->GetColumnDropOwnershipBundle(2) != source_j_ownership);
	auto added_values = scan_column(*added, StorageIndex(0), LogicalType::INTEGER);
	REQUIRE(added_values.size() == remaining_rows);
	REQUIRE(added_values.front() == Value::INTEGER(NumericCast<int32_t>(first->GetCount())));
	REQUIRE(added_values.back() == Value::INTEGER(4095));
	auto default_values = scan_column(*added, StorageIndex(2), LogicalType::INTEGER);
	REQUIRE(default_values.size() == remaining_rows);
	REQUIRE(default_values.front() == Value::INTEGER(99));
	REQUIRE(default_values.back() == Value::INTEGER(99));

	auto removed = collection->RemoveColumn(1);
	REQUIRE(removed->GetTotalRows() == remaining_rows);
	REQUIRE(removed->GetNextRowId() == 4096);
	REQUIRE(removed->HasLayoutHistory());
	REQUIRE(removed->GetCurrentLayout()->layout_version == 1);
	REQUIRE(removed->GetCurrentLayout()->patches.empty());
	auto removed_row_group = removed->GetRowGroup(0);
	REQUIRE(removed_row_group);
	REQUIRE(removed_row_group->GetColumnDropOwnershipBundle(0) == source_i_ownership);
	auto removed_values = scan_column(*removed, StorageIndex(0), LogicalType::INTEGER);
	REQUIRE(removed_values.size() == remaining_rows);
	REQUIRE(removed_values.front() == Value::INTEGER(NumericCast<int32_t>(first->GetCount())));
	REQUIRE(removed_values.back() == Value::INTEGER(4095));

	auto reference = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0);
	auto cast = BoundCastExpression::AddCastToType(*con.context, std::move(reference), LogicalType::BIGINT);
	auto changed = collection->AlterType(*con.context, 0, LogicalType::BIGINT, {StorageIndex(0)}, *cast,
	                                     TransactionData::Committed());
	REQUIRE(changed->GetTotalRows() == remaining_rows);
	REQUIRE(changed->GetNextRowId() == 4096);
	REQUIRE(changed->HasLayoutHistory());
	REQUIRE(changed->GetCurrentLayout()->layout_version == 1);
	REQUIRE(changed->GetCurrentLayout()->patches.empty());
	auto changed_row_group = changed->GetRowGroup(0);
	REQUIRE(changed_row_group);
	REQUIRE(changed_row_group->GetColumnDropOwnershipBundle(0) != source_i_ownership);
	REQUIRE(changed_row_group->GetColumnDropOwnershipBundle(1) == source_j_ownership);
	auto changed_values = scan_column(*changed, StorageIndex(0), LogicalType::BIGINT);
	REQUIRE(changed_values.size() == remaining_rows);
	REQUIRE(changed_values.front() == Value::BIGINT(NumericCast<int64_t>(first->GetCount())));
	REQUIRE(changed_values.back() == Value::BIGINT(4095));

	auto trailing_patch = MakeEmptyReplacementPatch(first->GetCount(), 4096, 2);
	collection->PublishLayout(make_shared_ptr<RowGroupLayout>(
	    2, 20, tree, duckdb::vector<duckdb::shared_ptr<const LayoutPatch>> {std::move(trailing_patch)}));
	auto trailing_gap = collection->RemoveColumn(1);
	REQUIRE(trailing_gap->GetTotalRows() == first->GetCount());
	REQUIRE(trailing_gap->GetNextRowId() == 4096);

	TableAppendState append_state;
	trailing_gap->InitializeAppend(TransactionData::Committed(), append_state, AppendOrganization::Unsorted());
	DataChunk append_chunk;
	append_chunk.Initialize(trailing_gap->GetAllocator(), {LogicalType::INTEGER});
	append_chunk.data[0].Append(Value::INTEGER(5000));
	append_chunk.SetChildCardinality(1);
	trailing_gap->Append(append_chunk, append_state);
	trailing_gap->FinalizeAppend(TransactionData::Committed(), append_state);
	auto appended_tree = trailing_gap->GetRowGroups();
	REQUIRE(appended_tree->GetSegmentCount() == 2);
	REQUIRE(appended_tree->GetSegmentByIndex(1)->GetRowStart() == 4096);
	REQUIRE(trailing_gap->GetTotalRows() == first->GetCount() + 1);
	REQUIRE(trailing_gap->GetNextRowId() == 4097);
}

TEST_CASE("Adaptive sorted writes preserve threshold and run boundaries", "[storage][row_group_layout]") {
	auto path = TestCreatePath("adaptive_sorted_write_layout.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS adaptive_layout (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE adaptive_layout"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER) SORTED BY (i)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (3), (1), (2)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 1);
		REQUIRE(collection->GetRowGroup(0)->GetSortMetadata() == RowGroupSortMetadata());
		REQUIRE(!collection->GetRowGroup(0)->IsSealed());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 1);
	});

	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (4095 - i)::INTEGER FROM range(4096) t(i)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 3);
		REQUIRE(collection->GetRowGroup(0)->GetSortMetadata() == RowGroupSortMetadata());
		for (idx_t row_group_idx = 1; row_group_idx < 3; row_group_idx++) {
			auto row_group = collection->GetRowGroup(NumericCast<int64_t>(row_group_idx));
			REQUIRE(row_group->GetSortMetadata() == RowGroupSortMetadata {1, 1});
			REQUIRE(row_group->IsSealed());
		}
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 2);
	});

	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT (2047 - i)::INTEGER FROM range(2048) t(i)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 4);
		REQUIRE(collection->GetRowGroup(3)->GetSortMetadata() == RowGroupSortMetadata {1, 2});
		REQUIRE(collection->GetRowGroup(3)->IsSealed());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 3);
	});

	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (6), (4), (5)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 5);
		REQUIRE(collection->GetRowGroup(4)->GetSortMetadata() == RowGroupSortMetadata());
		REQUIRE(!collection->GetRowGroup(4)->IsSealed());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 3);
	});

	REQUIRE_NO_FAIL(con.Query("SET threads = 4"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order = true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE batch_source AS "
	                          "SELECT (4095 - i)::INTEGER AS i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE batch_target(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO batch_target SELECT i FROM batch_source"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("batch_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 2);
		for (idx_t row_group_idx = 0; row_group_idx < 2; row_group_idx++) {
			auto row_group = collection->GetRowGroup(NumericCast<int64_t>(row_group_idx));
			REQUIRE(row_group->GetSortMetadata() == RowGroupSortMetadata {1, 1});
			REQUIRE(row_group->IsSealed());
		}
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 2);
	});

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE trigger_target(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE trigger_driver(marker INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TRIGGER sorted_target_writer AFTER INSERT ON trigger_driver FOR EACH STATEMENT "
	                          "INSERT INTO trigger_target SELECT (4095 - i)::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO trigger_driver VALUES (1)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("trigger_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 2);
		for (idx_t row_group_idx = 0; row_group_idx < 2; row_group_idx++) {
			REQUIRE(collection->GetRowGroup(NumericCast<int64_t>(row_group_idx))->GetSortMetadata() ==
			        RowGroupSortMetadata());
		}
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 1);
	});

	REQUIRE_NO_FAIL(con.Query("INSERT INTO trigger_target "
	                          "SELECT (2047 - i)::INTEGER FROM range(2048) t(i)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("trigger_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 3);
		REQUIRE(collection->GetRowGroup(2)->GetSortMetadata() == RowGroupSortMetadata {1, 1});
		REQUIRE(collection->GetRowGroup(2)->IsSealed());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 2);
	});
	DeleteDatabase(path);
}

TEST_CASE("Sorted table appenders preserve flush boundaries", "[storage][row_group_layout]") {
	auto path = TestCreatePath("sorted_appender_layout.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS sorted_appender (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE sorted_appender"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE public_target(i INTEGER) SORTED BY (i)"));

	Appender appender(con, "public_target");
	for (idx_t i = 0; i < 1024; i++) {
		appender.AppendRow(NumericCast<int32_t>(1023 - i));
	}
	appender.Flush();
	for (idx_t i = 0; i < 1024; i++) {
		appender.AppendRow(NumericCast<int32_t>(2047 - i));
	}
	appender.Flush();

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("public_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 1);
		REQUIRE(collection->GetRowGroup(0)->GetSortMetadata() == RowGroupSortMetadata());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 1);
	});

	for (idx_t i = 0; i < 2048; i++) {
		appender.AppendRow(NumericCast<int32_t>(4095 - i));
	}
	appender.Flush();
	for (idx_t i = 0; i < 2048; i++) {
		appender.AppendRow(NumericCast<int32_t>(6143 - i));
	}
	appender.Close();

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("public_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 3);
		REQUIRE(collection->GetRowGroup(0)->GetSortMetadata() == RowGroupSortMetadata());
		REQUIRE(collection->GetRowGroup(1)->GetSortMetadata() == RowGroupSortMetadata {1, 1});
		REQUIRE(collection->GetRowGroup(1)->IsSealed());
		REQUIRE(collection->GetRowGroup(2)->GetSortMetadata() == RowGroupSortMetadata {1, 2});
		REQUIRE(collection->GetRowGroup(2)->IsSealed());
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 3);
	});

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE internal_target(i INTEGER) SORTED BY (i)"));
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("internal_target")));
		MetaTransaction::Get(*con.context).ModifyDatabase(entry.GetStorage().db, DatabaseModificationType());
		InternalAppender internal_appender(*con.context, entry);
		for (idx_t i = 0; i < 4096; i++) {
			internal_appender.AppendRow(NumericCast<int32_t>(4095 - i));
		}
		internal_appender.Close();
	});

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("internal_target")));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		REQUIRE(collection->GetRowGroupCount() == 2);
		for (idx_t row_group_idx = 0; row_group_idx < 2; row_group_idx++) {
			REQUIRE(collection->GetRowGroup(NumericCast<int64_t>(row_group_idx))->GetSortMetadata() ==
			        RowGroupSortMetadata());
		}
		REQUIRE(entry.GetStorage().GetDataTableInfo()->GetSortStorage().next_run_id.load() == 1);
	});
	DeleteDatabase(path);
}

TEST_CASE("Sorted table ALTER waits for transaction write gates", "[storage][row_group_layout]") {
	auto path = TestCreatePath("sorted_write_gate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection writer(db);
	Connection ddl(db);
	REQUIRE_NO_FAIL(
	    writer.Query("ATTACH '" + path + "' AS write_gate (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(writer.Query("USE write_gate"));
	REQUIRE_NO_FAIL(ddl.Query("USE write_gate"));
	REQUIRE_NO_FAIL(writer.Query("CREATE TABLE target(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(writer.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(writer.Query("INSERT INTO target VALUES (1)"));

	std::atomic<bool> ddl_started(false);
	auto ddl_future = std::async(std::launch::async, [&]() {
		ddl_started = true;
		return ddl.Query("ALTER TABLE target ADD COLUMN payload BIGINT");
	});
	auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (!ddl_started.load() && std::chrono::steady_clock::now() < start_deadline) {
		std::this_thread::yield();
	}
	auto blocked_status = ddl_future.wait_for(std::chrono::milliseconds(100));
	auto commit_result = writer.Query("COMMIT");
	auto finished_status = ddl_future.wait_for(std::chrono::seconds(5));
	if (finished_status != std::future_status::ready) {
		ddl.Interrupt();
		ddl_future.wait();
	}

	REQUIRE(ddl_started.load());
	REQUIRE(blocked_status == std::future_status::timeout);
	REQUIRE_NO_FAIL(std::move(commit_result));
	REQUIRE(finished_status == std::future_status::ready);
	auto ddl_result = ddl_future.get();
	REQUIRE_NO_FAIL(std::move(ddl_result));
	REQUIRE_NO_FAIL(writer.Query("ALTER TABLE target RESET SORTED BY"));
	REQUIRE_NO_FAIL(writer.Query("UPDATE target SET i = 2"));
	DeleteDatabase(path);
}

TEST_CASE("Sorted table DROP waits for transaction write gates", "[storage][row_group_layout]") {
	auto path = TestCreatePath("sorted_drop_gate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection writer(db);
	Connection ddl(db);
	REQUIRE_NO_FAIL(writer.Query("ATTACH '" + path + "' AS drop_gate (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(writer.Query("USE drop_gate"));
	REQUIRE_NO_FAIL(ddl.Query("USE drop_gate"));
	REQUIRE_NO_FAIL(writer.Query("CREATE TABLE target(i INTEGER) SORTED BY (i)"));
	REQUIRE_NO_FAIL(writer.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(writer.Query("INSERT INTO target VALUES (1)"));

	std::atomic<bool> ddl_started(false);
	auto ddl_future = std::async(std::launch::async, [&]() {
		ddl_started = true;
		return ddl.Query("DROP TABLE target");
	});
	auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (!ddl_started.load() && std::chrono::steady_clock::now() < start_deadline) {
		std::this_thread::yield();
	}
	auto blocked_status = ddl_future.wait_for(std::chrono::milliseconds(100));
	auto commit_result = writer.Query("COMMIT");
	auto finished_status = ddl_future.wait_for(std::chrono::seconds(5));
	if (finished_status != std::future_status::ready) {
		ddl.Interrupt();
		ddl_future.wait();
	}

	REQUIRE(ddl_started.load());
	REQUIRE(blocked_status == std::future_status::timeout);
	REQUIRE_NO_FAIL(std::move(commit_result));
	REQUIRE(finished_status == std::future_status::ready);
	auto ddl_result = ddl_future.get();
	REQUIRE_NO_FAIL(std::move(ddl_result));
	REQUIRE_FAIL(writer.Query("SELECT * FROM target"));
	DeleteDatabase(path);
}

TEST_CASE("Sorted table SET rechecks indexes after DDL coordination", "[storage][row_group_layout]") {
	auto path = TestCreatePath("sorted_index_gate.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection gate_holder(db);
	Connection index_writer(db);
	REQUIRE_NO_FAIL(
	    gate_holder.Query("ATTACH '" + path + "' AS index_gate (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(gate_holder.Query("USE index_gate"));
	REQUIRE_NO_FAIL(index_writer.Query("USE index_gate"));
	REQUIRE_NO_FAIL(index_writer.Query("CREATE TABLE target(i INTEGER)"));
	REQUIRE_NO_FAIL(index_writer.Query("INSERT INTO target SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(gate_holder.Query("BEGIN TRANSACTION"));
	gate_holder.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*gate_holder.context, QualifiedName(Identifier("target")));
		auto &transaction = DuckTransaction::Get(*gate_holder.context, entry.GetStorage().db);
		transaction.HoldReclusterDDLCoordinationLock(*entry.GetStorage().GetDataTableInfo());
	});

	std::atomic<bool> index_started(false);
	auto index_future = std::async(std::launch::async, [&]() {
		index_started = true;
		return index_writer.Query("CREATE INDEX target_i ON target(i)");
	});
	auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (!index_started.load() && std::chrono::steady_clock::now() < start_deadline) {
		std::this_thread::yield();
	}
	auto blocked_status = index_future.wait_for(std::chrono::milliseconds(100));
	auto commit_result = gate_holder.Query("COMMIT");
	auto finished_status = index_future.wait_for(std::chrono::seconds(5));
	if (finished_status != std::future_status::ready) {
		index_writer.Interrupt();
		index_future.wait();
	}

	REQUIRE(index_started.load());
	REQUIRE(blocked_status == std::future_status::timeout);
	REQUIRE_NO_FAIL(std::move(commit_result));
	REQUIRE(finished_status == std::future_status::ready);
	auto index_result = index_future.get();
	REQUIRE_NO_FAIL(std::move(index_result));
	auto set_result = index_writer.Query("ALTER TABLE target SET SORTED BY (i)");
	REQUIRE_FAIL(set_result);
	REQUIRE(StringUtil::Contains(set_result->GetError(), "SORTED BY tables cannot have indexes"));
	DeleteDatabase(path);
}

TEST_CASE("Index DDL coordinates multiple ordinary tables in one transaction", "[storage][row_group_layout]") {
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE first(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE second(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO first VALUES (1), (2)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO second VALUES (1), (2)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX first_i ON first(i)"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE second ADD PRIMARY KEY(i)"));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_FAIL(con.Query("INSERT INTO first VALUES (1)"));
	REQUIRE_FAIL(con.Query("INSERT INTO second VALUES (1)"));
}
