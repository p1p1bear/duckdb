#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static DataTable &GetLocalAlterTable(Connection &con) {
	optional_ptr<DataTable> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		result = entry.GetStorage();
	});
	return *result;
}

static void VerifyFailedAlterPreservesLocalStorage(Connection &con, DataTable &table) {
	auto &transaction = DuckTransaction::Get(*con.context, table.GetAttached());
	auto local_table = transaction.GetLocalStorage().GetStorage(table);
	REQUIRE(local_table);
	REQUIRE(&local_table->table_ref.get() == &table);
	REQUIRE(local_table->GetCollection().GetTotalRows() == 1);
	REQUIRE(local_table->GetCollection().GetNextRowId() == 1);
}

TEST_CASE("Failed physical alters preserve transaction-local storage", "[storage][local_storage_alter]") {
	DuckDB db;
	Connection con(db);

	SECTION("unpublished ADD COLUMN replacement") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES (42)"));
		auto &table = GetLocalAlterTable(con);
		VerifyFailedAlterPreservesLocalStorage(con, table);

		ColumnDefinition new_column("j", LogicalType::INTEGER);
		BoundConstantExpression default_value(Value::INTEGER(99));
		{
			auto replacement = make_uniq<DataTable>(*con.context, table, new_column, default_value);
			VerifyFailedAlterPreservesLocalStorage(con, table);
		}
		VerifyFailedAlterPreservesLocalStorage(con, table);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}

	SECTION("ALTER TYPE cast failure") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl VALUES ('not-an-integer')"));
		auto &table = GetLocalAlterTable(con);
		VerifyFailedAlterPreservesLocalStorage(con, table);

		auto reference = make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 0);
		auto cast = BoundCastExpression::AddCastToType(*con.context, std::move(reference), LogicalType::INTEGER);
		REQUIRE_THROWS(make_uniq<DataTable>(*con.context, table, 0, LogicalType::INTEGER,
		                                    duckdb::vector<StorageIndex> {StorageIndex(0)}, *cast));
		VerifyFailedAlterPreservesLocalStorage(con, table);
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}
}
