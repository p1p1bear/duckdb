//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
class SchemaCatalogEntry;

struct CreateTableInfo : public CreateInfo {
	DUCKDB_API CreateTableInfo();
	DUCKDB_API explicit CreateTableInfo(QualifiedName qualified_name);
	DUCKDB_API CreateTableInfo(SchemaCatalogEntry &schema, const Identifier &name);

	//! Table name to insert to
	const Identifier &GetTableName() const {
		return qualified_name.Name();
	}
	void SetTableName(Identifier name) {
		qualified_name = qualified_name.WithName(std::move(name));
	}
	//! List of columns of the table
	ColumnList columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE as QUERY
	unique_ptr<SelectStatement> query;
	//! Table Partition definitions
	vector<unique_ptr<ParsedExpression>> partition_keys;
	//! Compatibility projection of table sort expressions for extension catalogs
	vector<unique_ptr<ParsedExpression>> sort_keys;
	//! Table Sort definitions including direction and NULL ordering
	vector<OrderByNode> sort_orders;
	//! Bound persistent sort metadata, present after a DuckDB catalog bind
	optional<TableSortCatalogMetadata> sort_metadata;
	//! Extra Table options if any
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	bool HasAnySortDefinition() const;
	void ValidateSortKeySources() const;
	void NormalizeSortKeys();
	string ExtraOptionsToString() const;
	string ToString() const override;
};

} // namespace duckdb
