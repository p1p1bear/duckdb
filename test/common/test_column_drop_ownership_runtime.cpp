#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_drop_ownership_runtime.hpp"
#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <exception>
#include <thread>
#include <type_traits>
#include <utility>

using namespace duckdb; // NOLINT

class ColumnDropOwnershipBlockCollector : public BlockIdVisitor {
public:
	void Visit(block_id_t block_id) override {
		blocks.push_back(block_id);
	}

	vector<block_id_t> blocks;
};

static vector<shared_ptr<RowGroupColumnDropOwnership>>
CaptureTableDropOwnership(Connection &con, const string &table_name, optional_idx selected_column = {}) {
	vector<shared_ptr<RowGroupColumnDropOwnership>> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto collection = entry.GetStorage().GetRowGroupCollection();
		auto row_group = collection->GetRowGroup(0);
		REQUIRE(row_group);
		auto first_column = selected_column.IsValid() ? selected_column.GetIndex() : 0;
		auto last_column = selected_column.IsValid() ? first_column + 1 : row_group->GetColumnCount();
		REQUIRE(last_column <= row_group->GetColumnCount());
		for (idx_t column_index = first_column; column_index < last_column; column_index++) {
			row_group->GetColumnDropOwnershipBundle(column_index);
			auto tree = CaptureColumnDropOwnershipRuntimeTree(row_group->GetRawColumnData(column_index));
			for (auto &node : tree.nodes) {
				auto token = node.get().GetDropOwnershipToken();
				REQUIRE(token);
				result.push_back(std::move(token));
			}
		}
	});
	return result;
}

class AliasedColumnDropOwnershipColumn : public ColumnData {
public:
	AliasedColumnDropOwnershipColumn(BlockManager &block_manager, DataTableInfo &info)
	    : ColumnData(block_manager, info, 0,
	                 LogicalType::STRUCT({{"left", LogicalType::INTEGER}, {"right", LogicalType::INTEGER}}),
	                 ColumnDataType::MAIN_TABLE, nullptr),
	      validity(ColumnData::CreateColumn(block_manager, info, 0, LogicalType(LogicalTypeId::VALIDITY))),
	      aliased_child(ColumnData::CreateColumn(block_manager, info, 0, LogicalType::INTEGER)) {
	}

	ColumnDropOwnershipRuntimeKind GetDropOwnershipRuntimeKind() const noexcept override {
		return ColumnDropOwnershipRuntimeKind::STRUCT;
	}

	void VisitDropOwnershipChildren(ColumnDropOwnershipChildVisitor &visitor) override {
		visitor.Visit(ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VALIDITY, 0), *validity);
		visitor.Visit(ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::STRUCT_FIELD, 0), *aliased_child);
		visitor.Visit(ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::STRUCT_FIELD, 1), *aliased_child);
	}

private:
	shared_ptr<ColumnData> validity;
	shared_ptr<ColumnData> aliased_child;
};

static_assert(noexcept(std::declval<ColumnDropOwnershipRuntimeTree &>().ApplyTokenPlan(
                  std::declval<const vector<shared_ptr<RowGroupColumnDropOwnership>> &>())),
              "applying a validated ownership token plan must be noexcept");

TEST_CASE("Ordinary row groups initialize column ownership only when shared", "[storage][drop_ownership_runtime]") {
	auto path = TestCreatePath("column_drop_ownership_lazy.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER, removed VARCHAR, retained BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER, i::VARCHAR, i::BIGINT FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			auto source_row_group = collection->GetRowGroup(0);
			REQUIRE(source_row_group);
			for (idx_t column_index = 0; column_index < source_row_group->GetColumnCount(); column_index++) {
				REQUIRE_FALSE(source_row_group->HasColumnDropOwnershipBundle(column_index));
			}

			auto unloaded_tree = CaptureColumnDropOwnershipRuntimeTree(source_row_group->GetRawColumnData(0));
			REQUIRE_FALSE(source_row_group->HasColumnDropOwnershipBundle(0));
			for (auto &node : unloaded_tree.nodes) {
				REQUIRE_FALSE(node.get().GetDropOwnershipToken());
			}

			auto removed_collection = collection->RemoveColumn(1);
			auto removed_row_group = removed_collection->GetRowGroup(0);
			REQUIRE(removed_row_group);
			REQUIRE(source_row_group->HasColumnDropOwnershipBundle(0));
			REQUIRE_FALSE(source_row_group->HasColumnDropOwnershipBundle(1));
			REQUIRE(source_row_group->HasColumnDropOwnershipBundle(2));
			REQUIRE(removed_row_group->HasColumnDropOwnershipBundle(0));
			REQUIRE(removed_row_group->HasColumnDropOwnershipBundle(1));
			REQUIRE(source_row_group->GetColumnDropOwnershipBundle(0) ==
			        removed_row_group->GetColumnDropOwnershipBundle(0));
			REQUIRE(source_row_group->GetColumnDropOwnershipBundle(2) ==
			        removed_row_group->GetColumnDropOwnershipBundle(1));

			auto source_tree = CaptureColumnDropOwnershipRuntimeTree(source_row_group->GetRawColumnData(2));
			auto clone_tree = CaptureColumnDropOwnershipRuntimeTree(removed_row_group->GetRawColumnData(1));
			REQUIRE(source_tree.nodes.size() == clone_tree.nodes.size());
			for (idx_t node_index = 0; node_index < source_tree.nodes.size(); node_index++) {
				REQUIRE(source_tree.nodes[node_index].get().GetDropOwnershipToken());
				REQUIRE(source_tree.nodes[node_index].get().GetDropOwnershipToken() ==
				        clone_tree.nodes[node_index].get().GetDropOwnershipToken());
			}
		});
	}
	DeleteDatabase(path);
}

TEST_CASE("Nested checkpoint results preserve ownership for reused children", "[storage][drop_ownership_runtime]") {
	auto path = TestCreatePath("column_drop_ownership_nested_checkpoint.db");
	DeleteDatabase(path);
	shared_ptr<RowGroup> source_row_group;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(payload STRUCT(a INTEGER, b BIGINT))"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT struct_pack(a := i::INTEGER, b := i::BIGINT) "
		                          "FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto root = entry.GetStorage().GetRowGroupCollection()->GetRowGroups()->GetRootSegment();
			REQUIRE(root);
			source_row_group = root->ReferenceNode();
			REQUIRE(source_row_group);
			REQUIRE_FALSE(source_row_group->HasColumnDropOwnershipBundle(0));

			auto &table = entry.GetStorage();
			MetaTransaction::Get(*con.context).ModifyDatabase(table.db, DatabaseModificationType());
			Vector row_ids(LogicalType::ROW_TYPE);
			row_ids.Append(Value::BIGINT(0));
			DataChunk updates;
			updates.Initialize(*con.context, {LogicalType::INTEGER});
			updates.data[0].Append(Value::INTEGER(42));
			updates.SetChildCardinality(1);
			table.UpdateColumn(entry, *con.context, row_ids, {0, 1}, updates);
			REQUIRE_FALSE(source_row_group->HasColumnDropOwnershipBundle(0));
		});

		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		shared_ptr<RowGroup> current_row_group;
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto root = entry.GetStorage().GetRowGroupCollection()->GetRowGroups()->GetRootSegment();
			REQUIRE(root);
			current_row_group = root->ReferenceNode();
		});
		REQUIRE(current_row_group);
		REQUIRE(current_row_group != source_row_group);
		REQUIRE(source_row_group->HasColumnDropOwnershipBundle(0));
		REQUIRE(current_row_group->HasColumnDropOwnershipBundle(0));
		REQUIRE(source_row_group->GetColumnDropOwnershipBundle(0) !=
		        current_row_group->GetColumnDropOwnershipBundle(0));

		auto source_tree = CaptureColumnDropOwnershipRuntimeTree(source_row_group->GetRawColumnData(0));
		auto current_tree = CaptureColumnDropOwnershipRuntimeTree(current_row_group->GetRawColumnData(0));
		REQUIRE(source_tree.nodes.size() == current_tree.nodes.size());
		bool found_reused_child = false;
		bool found_rewritten_node = false;
		for (idx_t node_index = 0; node_index < source_tree.nodes.size(); node_index++) {
			auto source_token = source_tree.nodes[node_index].get().GetDropOwnershipToken();
			auto current_token = current_tree.nodes[node_index].get().GetDropOwnershipToken();
			REQUIRE(source_token);
			REQUIRE(current_token);
			found_reused_child |= source_token == current_token;
			found_rewritten_node |= source_token != current_token;
		}
		REQUIRE(found_reused_child);
		REQUIRE(found_rewritten_node);

		auto rows = con.Query("SELECT (payload).a, (payload).b FROM tbl WHERE rowid = 0");
		REQUIRE(rows);
		REQUIRE(CHECK_COLUMN(rows, 0, {42}));
		REQUIRE(CHECK_COLUMN(rows, 1, {0}));
		source_row_group.reset();
	}
	DeleteDatabase(path);
}

TEST_CASE("Persistent nested columns expose direct drop ownership nodes", "[storage][drop_ownership_runtime]") {
	auto path = TestCreatePath("column_drop_ownership_runtime.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(payload VARCHAR[], removed INTEGER, retained BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT [repeat(i::VARCHAR, 64), "
		                          "repeat((i + 1)::VARCHAR, 64)], i::INTEGER, i::BIGINT FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET experimental_metadata_reuse=true"));
		shared_ptr<RowGroup> pre_checkpoint_row_group;
		vector<shared_ptr<ColumnDropOwnershipBundle>> pre_checkpoint_bundles;
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			auto root = collection->GetRowGroups()->GetRootSegment();
			REQUIRE(root);
			pre_checkpoint_row_group = root->ReferenceNode();
			for (idx_t column_index = 0; column_index < pre_checkpoint_row_group->GetColumnCount(); column_index++) {
				pre_checkpoint_bundles.push_back(pre_checkpoint_row_group->GetColumnDropOwnershipBundle(column_index));
			}

			auto removed_collection = collection->RemoveColumn(1);
			auto removed_root = removed_collection->GetRowGroups()->GetRootSegment();
			REQUIRE(removed_root);
			auto &removed_row_group = removed_root->GetNode();
			REQUIRE(removed_row_group.GetColumnDropOwnershipBundle(0) == pre_checkpoint_bundles[0]);
			REQUIRE(removed_row_group.GetColumnDropOwnershipBundle(1) == pre_checkpoint_bundles[2]);

			atomic<idx_t> ready_count {0};
			atomic<bool> start {false};
			std::exception_ptr source_error;
			std::exception_ptr clone_error;
			auto load_column = [&](RowGroup &row_group, std::exception_ptr &error) {
				ready_count.fetch_add(1);
				while (!start.load()) {
					std::this_thread::yield();
				}
				try {
					row_group.GetRawColumnData(0);
				} catch (...) {
					error = std::current_exception();
				}
			};
			std::thread source_loader(load_column, std::ref(*pre_checkpoint_row_group), std::ref(source_error));
			std::thread clone_loader(load_column, std::ref(removed_row_group), std::ref(clone_error));
			while (ready_count.load() != 2) {
				std::this_thread::yield();
			}
			start = true;
			source_loader.join();
			clone_loader.join();
			REQUIRE_FALSE(source_error);
			REQUIRE_FALSE(clone_error);

			auto &column = pre_checkpoint_row_group->GetRawColumnData(0);
			auto &clone_column = removed_row_group.GetRawColumnData(0);

			ColumnDropOwnershipBlockCollector direct_blocks;
			ColumnDropOwnershipBlockCollector recursive_blocks;
			column.VisitDirectBlockIds(direct_blocks);
			column.VisitBlockIds(recursive_blocks);
			REQUIRE_FALSE(direct_blocks.blocks.empty());
			REQUIRE(recursive_blocks.blocks.size() > direct_blocks.blocks.size());
			for (idx_t block_index = 0; block_index < direct_blocks.blocks.size(); block_index++) {
				REQUIRE(recursive_blocks.blocks[block_index] == direct_blocks.blocks[block_index]);
			}

			auto runtime_tree = CaptureColumnDropOwnershipRuntimeTree(column);
			REQUIRE(runtime_tree.shape);
			REQUIRE(runtime_tree.shape->NodeCount() == 4);
			REQUIRE(runtime_tree.nodes.size() == 4);
			auto &shape_nodes = runtime_tree.shape->GetNodes();
			REQUIRE(shape_nodes[0].layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::LIST);
			REQUIRE(shape_nodes[0].child_key.role == ColumnDropOwnershipChildRole::ROOT);
			REQUIRE(shape_nodes[1].layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::VALIDITY);
			REQUIRE(shape_nodes[1].child_key.role == ColumnDropOwnershipChildRole::VALIDITY);
			REQUIRE(shape_nodes[1].parent_index == 0);
			REQUIRE(shape_nodes[2].layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::STANDARD);
			REQUIRE(shape_nodes[2].layout_tag.logical_type == LogicalType::VARCHAR);
			REQUIRE(shape_nodes[2].child_key.role == ColumnDropOwnershipChildRole::ELEMENT);
			REQUIRE(shape_nodes[2].parent_index == 0);
			REQUIRE(shape_nodes[3].layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::VALIDITY);
			REQUIRE(shape_nodes[3].child_key.role == ColumnDropOwnershipChildRole::VALIDITY);
			REQUIRE(shape_nodes[3].parent_index == 2);

			vector<shared_ptr<RowGroupColumnDropOwnership>> canonical_tokens(runtime_tree.nodes.size());
			auto bundle = pre_checkpoint_row_group->GetColumnDropOwnershipBundle(0);
			REQUIRE(bundle->Bind(runtime_tree.shape, canonical_tokens) == ColumnDropOwnershipBindResult::VERIFIED);
			REQUIRE(runtime_tree.ApplyTokenPlan(canonical_tokens));
			for (idx_t node_index = 0; node_index < runtime_tree.nodes.size(); node_index++) {
				REQUIRE(canonical_tokens[node_index]);
				REQUIRE(runtime_tree.nodes[node_index].get().GetDropOwnershipToken() == canonical_tokens[node_index]);
			}

			auto rebound_tree = CaptureColumnDropOwnershipRuntimeTree(clone_column);
			vector<shared_ptr<RowGroupColumnDropOwnership>> rebound_tokens(rebound_tree.nodes.size());
			REQUIRE(bundle->Bind(rebound_tree.shape, rebound_tokens) == ColumnDropOwnershipBindResult::VERIFIED);
			REQUIRE(rebound_tree.ApplyTokenPlan(rebound_tokens));
			REQUIRE(rebound_tokens == canonical_tokens);

			auto invalid_tokens = canonical_tokens;
			invalid_tokens[2] = make_shared_ptr<RowGroupColumnDropOwnership>();
			REQUIRE_FALSE(rebound_tree.ApplyTokenPlan(invalid_tokens));
			invalid_tokens = canonical_tokens;
			invalid_tokens[1].reset();
			REQUIRE_FALSE(rebound_tree.ApplyTokenPlan(invalid_tokens));
			for (idx_t node_index = 0; node_index < rebound_tree.nodes.size(); node_index++) {
				REQUIRE(rebound_tree.nodes[node_index].get().GetDropOwnershipToken() == canonical_tokens[node_index]);
			}

			auto incomplete =
			    ColumnData::CreateColumn(column.GetBlockManager(), column.GetTableInfo(), 0,
			                             LogicalType::LIST(LogicalType::VARCHAR), ColumnDataType::CHECKPOINT_TARGET);
			REQUIRE_THROWS_AS(CaptureColumnDropOwnershipRuntimeTree(*incomplete), InternalException);

			AliasedColumnDropOwnershipColumn aliased(column.GetBlockManager(), column.GetTableInfo());
			REQUIRE_THROWS_AS(CaptureColumnDropOwnershipRuntimeTree(aliased), InternalException);
		});

		REQUIRE_NO_FAIL(con.Query("UPDATE tbl SET retained = retained + 1"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			auto collection = entry.GetStorage().GetRowGroupCollection();
			auto root = collection->GetRowGroups()->GetRootSegment();
			REQUIRE(root);
			auto current_row_group = root->ReferenceNode();
			REQUIRE(current_row_group != pre_checkpoint_row_group);
			REQUIRE(current_row_group->GetColumnDropOwnershipBundle(0) == pre_checkpoint_bundles[0]);
			REQUIRE(current_row_group->GetColumnDropOwnershipBundle(1) == pre_checkpoint_bundles[1]);
			REQUIRE(current_row_group->GetColumnDropOwnershipBundle(2) != pre_checkpoint_bundles[2]);

			auto old_changed_tree =
			    CaptureColumnDropOwnershipRuntimeTree(pre_checkpoint_row_group->GetRawColumnData(2));
			auto new_changed_tree = CaptureColumnDropOwnershipRuntimeTree(current_row_group->GetRawColumnData(2));
			REQUIRE(old_changed_tree.nodes.size() == new_changed_tree.nodes.size());
			REQUIRE(old_changed_tree.nodes[0].get().GetDropOwnershipToken() !=
			        new_changed_tree.nodes[0].get().GetDropOwnershipToken());
		});
	}
	DeleteDatabase(path);
}

TEST_CASE("Column ownership drops revert before WAL commit and finalize once", "[storage][drop_ownership_runtime]") {
	auto path = TestCreatePath("column_drop_ownership_commit.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(payload VARCHAR[], retained BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT [repeat(i::VARCHAR, 64)], i FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto tokens = CaptureTableDropOwnership(con, "tbl", 0);
		REQUIRE(tokens.size() > 1);
		for (auto &token : tokens) {
			REQUIRE(token->GetState() == RowGroupColumnDropOwnership::State::LIVE);
		}

		REQUIRE_NO_FAIL(con.Query("SET debug_force_commit_failure=true"));
		auto failed = con.Query("ALTER TABLE tbl DROP COLUMN payload");
		REQUIRE_FAIL(failed);
		REQUIRE(failed->GetError().find("Forced commit failure") != string::npos);
		REQUIRE_NO_FAIL(con.Query("SET debug_force_commit_failure=false"));
		for (auto &token : tokens) {
			REQUIRE(token->GetState() == RowGroupColumnDropOwnership::State::LIVE);
		}
		auto rows = con.Query("SELECT count(*), sum(retained) FROM tbl");
		REQUIRE(rows);
		REQUIRE(CHECK_COLUMN(rows, 0, {4096}));
		REQUIRE(CHECK_COLUMN(rows, 1, {8386560}));

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl DROP COLUMN payload"));
		for (auto &token : tokens) {
			REQUIRE(token->GetState() == RowGroupColumnDropOwnership::State::DROPPED);
		}
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto rows = con.Query("SELECT count(*), sum(retained) FROM tbl");
		REQUIRE(rows);
		REQUIRE(CHECK_COLUMN(rows, 0, {4096}));
		REQUIRE(CHECK_COLUMN(rows, 1, {8386560}));
	}
	DeleteDatabase(path);
}

TEST_CASE("Table drop applies column ownership before index cleanup", "[storage][drop_ownership_runtime]") {
	auto path = TestCreatePath("column_drop_ownership_index.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(id INTEGER, payload VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i, repeat(i::VARCHAR, 64) FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX tbl_id ON tbl(id)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto tokens = CaptureTableDropOwnership(con, "tbl");
		REQUIRE_FALSE(tokens.empty());

		REQUIRE_NO_FAIL(con.Query("DROP TABLE tbl"));
		for (auto &token : tokens) {
			REQUIRE(token->GetState() == RowGroupColumnDropOwnership::State::DROPPED);
		}
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_FAIL(con.Query("SELECT * FROM tbl"));
	}
	DeleteDatabase(path);
}
