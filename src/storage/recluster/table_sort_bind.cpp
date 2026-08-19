#include "duckdb/storage/recluster/table_sort_bind.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

SortOrderDefinition BindPersistentSortDefinition(const vector<OrderByNode> &orders, const ColumnList &columns,
                                                 sort_order_id_t sort_order_id) {
	if (sort_order_id == INVALID_SORT_ORDER_ID) {
		throw InternalException("Cannot bind a persistent sort definition with an invalid ID");
	}
	if (orders.empty()) {
		throw BinderException("SORTED BY requires at least one column");
	}

	SortOrderDefinition result;
	result.sort_order_id = sort_order_id;
	unordered_set<persistent_column_id_t> seen_columns;
	for (auto &order : orders) {
		if (order.type != OrderType::ORDER_DEFAULT && order.type != OrderType::ASCENDING) {
			throw BinderException("SORTED BY only supports ASC sort order");
		}
		if (order.null_order != OrderByNullType::ORDER_DEFAULT && order.null_order != OrderByNullType::NULLS_LAST) {
			throw BinderException("SORTED BY only supports NULLS LAST");
		}
		if (order.expression->GetExpressionType() != ExpressionType::COLUMN_REF) {
			throw BinderException("SORTED BY only supports physical column references");
		}
		auto &column_ref = order.expression->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw BinderException("SORTED BY only supports unqualified column references");
		}
		auto &column_name = column_ref.GetColumnName();
		if (!columns.ColumnExists(column_name)) {
			throw BinderException("SORTED BY column \"%s\" does not exist", column_name.GetIdentifierName());
		}
		auto &column = columns.GetColumn(column_name);
		if (column.Generated()) {
			throw BinderException("SORTED BY column \"%s\" must be a physical column", column.Name());
		}
		auto column_id = column.PersistentColumnId();
		if (column_id == 0) {
			throw InternalException("SORTED BY column \"%s\" does not have a persistent column ID", column.Name());
		}
		if (!seen_columns.insert(column_id).second) {
			throw BinderException("SORTED BY column \"%s\" is specified more than once", column.Name());
		}
		result.columns.push_back({column_id, OrderType::ASCENDING, OrderByNullType::NULLS_LAST});
	}
	return result;
}

void ValidateTableSortCatalogMetadata(const TableSortCatalogMetadata &metadata, const ColumnList &columns) {
	if (metadata.table_id == hugeint_t(0, 0)) {
		throw SerializationException("SORTED BY table metadata has an invalid table ID");
	}

	unordered_set<persistent_column_id_t> current_column_ids;
	persistent_column_id_t max_column_id = 0;
	for (auto &column : columns.Logical()) {
		auto column_id = column.PersistentColumnId();
		if (column.Generated()) {
			if (column_id != 0) {
				throw SerializationException("Generated column \"%s\" has a persistent column ID", column.Name());
			}
			continue;
		}
		if (column_id == 0) {
			throw SerializationException("Physical column \"%s\" has no persistent column ID", column.Name());
		}
		if (!current_column_ids.insert(column_id).second) {
			throw SerializationException("Persistent column ID %llu is used more than once", column_id);
		}
		max_column_id = MaxValue(max_column_id, column_id);
	}
	unordered_set<sort_order_id_t> definition_ids;
	sort_order_id_t max_sort_order_id = 0;
	for (auto &definition : metadata.definitions) {
		if (definition.sort_order_id == INVALID_SORT_ORDER_ID ||
		    !definition_ids.insert(definition.sort_order_id).second) {
			throw SerializationException("SORTED BY table metadata has duplicate or invalid sort order IDs");
		}
		if (definition.columns.empty()) {
			throw SerializationException("Persistent sort definition %llu has no columns", definition.sort_order_id);
		}
		unordered_set<persistent_column_id_t> definition_columns;
		for (auto &sort_column : definition.columns) {
			if (sort_column.column_id == 0 || !definition_columns.insert(sort_column.column_id).second) {
				throw SerializationException("Persistent sort definition %llu has duplicate or invalid column IDs",
				                             definition.sort_order_id);
			}
			if (sort_column.order_type != OrderType::ASCENDING ||
			    sort_column.null_order != OrderByNullType::NULLS_LAST) {
				throw SerializationException("Persistent sort definition %llu uses unsupported ordering",
				                             definition.sort_order_id);
			}
			max_column_id = MaxValue(max_column_id, sort_column.column_id);
		}
		max_sort_order_id = MaxValue(max_sort_order_id, definition.sort_order_id);
	}
	if (metadata.next_column_id == 0 || metadata.next_column_id <= max_column_id) {
		throw SerializationException("SORTED BY table metadata has an invalid next column ID");
	}
	if (metadata.next_sort_order_id == INVALID_SORT_ORDER_ID || metadata.next_sort_order_id <= max_sort_order_id) {
		throw SerializationException("SORTED BY table metadata has an invalid next sort order ID");
	}
	if (!metadata.IsEnabled()) {
		return;
	}
	auto current = metadata.GetCurrent();
	if (!current) {
		throw SerializationException("SORTED BY table metadata has no current sort definition");
	}
	for (auto &sort_column : current->columns) {
		if (current_column_ids.find(sort_column.column_id) == current_column_ids.end()) {
			throw SerializationException("Current sort definition references missing column ID %llu",
			                             sort_column.column_id);
		}
	}
}

} // namespace duckdb
