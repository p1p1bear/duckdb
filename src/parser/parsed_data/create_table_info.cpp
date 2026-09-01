#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

CreateTableInfo::CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, Identifier::InvalidSchema()) {
}

CreateTableInfo::CreateTableInfo(QualifiedName qualified_name_p) : CreateInfo(CatalogType::TABLE_ENTRY) {
	SetQualifiedName(std::move(qualified_name_p));
}

CreateTableInfo::CreateTableInfo(SchemaCatalogEntry &schema, const Identifier &name_p)
    : CreateTableInfo(schema.GetQualifiedName(name_p)) {
}

unique_ptr<CreateInfo> CreateTableInfo::Copy() const {
	auto result = make_uniq<CreateTableInfo>(GetQualifiedName());
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	for (auto &partition : partition_keys) {
		result->partition_keys.push_back(partition->Copy());
	}
	for (auto &order : sort_keys) {
		result->sort_keys.push_back(order->Copy());
	}
	for (auto &order : sort_orders) {
		result->sort_orders.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	result->sort_metadata = sort_metadata;
	for (auto &option : options) {
		result->options.emplace(option.first, option.second->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	result->NormalizeLegacySortKeys();
	return std::move(result);
}

bool CreateTableInfo::HasAnySortDefinition() const {
	return !sort_keys.empty() || !sort_orders.empty() || sort_metadata.has_value();
}

void CreateTableInfo::ValidateSortKeySources() const {
	if (sort_metadata) {
		if (!sort_keys.empty() || !sort_orders.empty()) {
			throw SerializationException("Persistent table sort metadata cannot be combined with parser sort keys");
		}
		return;
	}
	if (sort_keys.empty() || sort_orders.empty()) {
		return;
	}
	if (sort_keys.size() != sort_orders.size()) {
		throw SerializationException("Table sort key projections have different lengths");
	}
	for (idx_t i = 0; i < sort_keys.size(); i++) {
		if (!ParsedExpression::Equals(sort_keys[i], sort_orders[i].expression)) {
			throw SerializationException("Table sort key projections do not match");
		}
	}
}

void CreateTableInfo::NormalizeLegacySortKeys() {
	ValidateSortKeySources();
	if (sort_orders.empty()) {
		for (auto &sort_key : sort_keys) {
			sort_orders.emplace_back(OrderType::ORDER_DEFAULT, OrderByNullType::ORDER_DEFAULT, sort_key->Copy());
		}
	}
	sort_keys.clear();
}

void CreateTableInfo::Serialize(Serializer &serializer) const {
	ValidateSortKeySources();
	if (sort_metadata && !serializer.ShouldSerialize(MIN_SORTED_BY_STORAGE_VERSION)) {
		throw SerializationException("Persistent SORTED BY metadata requires storage version v2.0.0 or newer");
	}
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<Identifier>(200, "table", qualified_name.Name());
	serializer.WriteProperty<ColumnList>(201, "columns", columns);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Constraint>>>(202, "constraints", constraints);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", query);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "partition_keys", partition_keys);

	vector<unique_ptr<ParsedExpression>> legacy_sort_keys;
	const auto serialize_sort_orders = serializer.ShouldSerialize(MIN_SORTED_BY_STORAGE_VERSION);
	if (!serialize_sort_orders && !sort_orders.empty()) {
		for (auto &sort_order : sort_orders) {
			legacy_sort_keys.push_back(sort_order.expression->Copy());
		}
	} else if (sort_orders.empty()) {
		for (auto &sort_key : sort_keys) {
			legacy_sort_keys.push_back(sort_key->Copy());
		}
	}
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(205, "sort_keys", legacy_sort_keys);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(206, "options", options);
	if (!serialize_sort_orders) {
		return;
	}
	serializer.WritePropertyWithDefault<optional<TableSortCatalogMetadata>>(207, "sort_metadata", sort_metadata,
	                                                                        optional<TableSortCatalogMetadata>());
	serializer.WritePropertyWithDefault<vector<OrderByNode>>(208, "sort_orders", sort_orders);
}

unique_ptr<CreateInfo> CreateTableInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CreateTableInfo>();
	auto table = deserializer.ReadPropertyWithDefault<Identifier>(200, "table");
	deserializer.ReadProperty<ColumnList>(201, "columns", result->columns);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Constraint>>>(202, "constraints", result->constraints);
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", result->query);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "partition_keys",
	                                                                           result->partition_keys);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(205, "sort_keys", result->sort_keys);
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(206, "options",
	                                                                                           result->options);
	deserializer.ReadPropertyWithExplicitDefault<optional<TableSortCatalogMetadata>>(
	    207, "sort_metadata", result->sort_metadata, optional<TableSortCatalogMetadata>());
	deserializer.ReadPropertyWithDefault<vector<OrderByNode>>(208, "sort_orders", result->sort_orders);
	result->SetName(std::move(table));
	result->NormalizeLegacySortKeys();
	return std::move(result);
}

static const ColumnDefinition &GetPersistentSortColumn(const ColumnList &columns, persistent_column_id_t column_id) {
	for (auto &column : columns.Physical()) {
		if (column.PersistentColumnId() == column_id) {
			return column;
		}
	}
	throw InternalException("Persistent sort definition references unknown column ID %llu", column_id);
}

static string PersistentSortColumnToString(const ColumnList &columns, const SortColumnDefinition &sort_column) {
	string result;
	result += SQLIdentifier(GetPersistentSortColumn(columns, sort_column.column_id).Name());
	if (sort_column.order_type == OrderType::ASCENDING) {
		result += " ASC";
	} else if (sort_column.order_type == OrderType::DESCENDING) {
		result += " DESC";
	} else {
		throw InternalException("Persistent sort definition contains an invalid order type");
	}
	if (sort_column.null_order == OrderByNullType::NULLS_FIRST) {
		result += " NULLS FIRST";
	} else if (sort_column.null_order == OrderByNullType::NULLS_LAST) {
		result += " NULLS LAST";
	} else {
		throw InternalException("Persistent sort definition contains an invalid NULL order");
	}
	return result;
}

string CreateTableInfo::ExtraOptionsToString() const {
	string ret;
	if (!partition_keys.empty()) {
		ret += " PARTITIONED BY (";
		for (auto &partition : partition_keys) {
			ret += partition->ToString() + ",";
		}
		ret.pop_back();
		ret += ")";
	}
	if (sort_metadata) {
		if (sort_metadata->IsEnabled()) {
			auto current = sort_metadata->GetCurrent();
			if (!current || current->columns.empty()) {
				throw InternalException("Current persistent sort definition is missing or empty");
			}
			ret += " SORTED BY (";
			for (idx_t i = 0; i < current->columns.size(); i++) {
				if (i > 0) {
					ret += ",";
				}
				ret += PersistentSortColumnToString(columns, current->columns[i]);
			}
			ret += ")";
		}
	} else if (!sort_orders.empty()) {
		ret += " SORTED BY (";
		for (auto &order : sort_orders) {
			ret += order.ToString() + ",";
		}
		ret.pop_back();
		ret += ")";
	} else if (!sort_keys.empty()) {
		ret += " SORTED BY (";
		for (auto &order : sort_keys) {
			ret += order->ToString() + ",";
		}
		ret.pop_back();
		ret += ")";
	}
	if (!options.empty()) {
		ret += " WITH (";
		for (auto &entry : options) {
			ret += "'" + entry.first + "'=" + entry.second->ToString() + ",";
		}
		ret.pop_back();
		ret += ")";
	}
	return ret;
}

string CreateTableInfo::ToString() const {
	string ret = GetCreatePrefix("TABLE");
	ret += QualifiedNameToString();

	if (query != nullptr) {
		ret += TableCatalogEntry::ColumnNamesToSQL(columns);
		ret += ExtraOptionsToString();
		ret += " AS " + query->ToString();
	} else {
		ret += TableCatalogEntry::ColumnsToSQL(columns, constraints);
		ret += ExtraOptionsToString();
		ret += ";";
	}
	return ret;
}

} // namespace duckdb
