#include "duckdb/storage/recluster/table_sort_bind.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

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

TableSortCatalogMetadata CreateTableSortIdentity(ColumnList &columns) {
	TableSortCatalogMetadata metadata;
	do {
		metadata.table_id = UUID::GenerateRandomUUID();
	} while (metadata.table_id == hugeint_t(0, 0));
	for (idx_t column_idx = 0; column_idx < columns.LogicalColumnCount(); column_idx++) {
		auto &column = columns.GetColumnMutable(LogicalIndex(column_idx));
		if (column.Generated()) {
			column.SetPersistentColumnId(0);
			continue;
		}
		column.SetPersistentColumnId(metadata.next_column_id++);
	}
	return metadata;
}

TableSortCatalogPostImage BuildTableSortPostImage(const TableSortCatalogMetadata &metadata, const ColumnList &columns) {
	TableSortCatalogPostImage result;
	result.table_metadata = metadata;
	for (idx_t column_idx = 0; column_idx < columns.LogicalColumnCount(); column_idx++) {
		auto &column = columns.GetColumn(LogicalIndex(column_idx));
		result.columns.push_back(
		    {column_idx, column.Name().GetIdentifierName(), column.Type(), column.PersistentColumnId()});
	}
	return result;
}

void ApplyTableSortPostImage(const TableSortCatalogPostImage &post_image, ColumnList &columns,
                             optional<TableSortCatalogMetadata> &metadata) {
	if (post_image.columns.size() != columns.LogicalColumnCount()) {
		throw SerializationException("SORTED BY ALTER post-image has an invalid column count");
	}
	vector<bool> seen(columns.LogicalColumnCount(), false);
	for (auto &assignment : post_image.columns) {
		if (assignment.logical_column_index >= columns.LogicalColumnCount() || seen[assignment.logical_column_index]) {
			throw SerializationException("SORTED BY ALTER post-image has duplicate or invalid column indexes");
		}
		seen[assignment.logical_column_index] = true;
		auto &column = columns.GetColumnMutable(LogicalIndex(assignment.logical_column_index));
		if (column.Name().GetIdentifierName() != assignment.name || column.Type() != assignment.type) {
			throw SerializationException("SORTED BY ALTER post-image does not match column %llu",
			                             assignment.logical_column_index);
		}
		column.SetPersistentColumnId(assignment.column_id);
	}
	ValidateTableSortCatalogMetadata(post_image.table_metadata, columns);
	metadata = post_image.table_metadata;
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

vector<idx_t> BindPersistentSortIndexes(const vector<ColumnDefinition> &columns,
                                        const SortOrderDefinition &definition) {
	vector<idx_t> result;
	result.reserve(definition.columns.size());
	for (auto &sort_column : definition.columns) {
		optional_idx physical_index;
		for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
			if (columns[column_index].PersistentColumnId() == sort_column.column_id) {
				physical_index = column_index;
				break;
			}
		}
		if (!physical_index.IsValid()) {
			throw InternalException("Current SORTED BY definition references a missing storage column ID");
		}
		result.push_back(physical_index.GetIndex());
	}
	return result;
}

vector<BoundOrderByNode> BuildPersistentSortOrders(const SortOrderDefinition &definition,
                                                   const vector<idx_t> &physical_indexes,
                                                   const vector<LogicalType> &input_types,
                                                   idx_t physical_column_count) {
	if (definition.columns.empty() || definition.columns.size() != physical_indexes.size() ||
	    physical_column_count > input_types.size()) {
		throw InternalException("Invalid persistent sort definition");
	}

	vector<BoundOrderByNode> result;
	result.reserve(physical_indexes.size());
	for (idx_t sort_index = 0; sort_index < physical_indexes.size(); sort_index++) {
		auto column_index = physical_indexes[sort_index];
		if (column_index >= physical_column_count) {
			throw InternalException("SORTED BY column is outside the physical table columns");
		}
		auto &sort_column = definition.columns[sort_index];
		result.emplace_back(sort_column.order_type, sort_column.null_order,
		                    make_uniq<BoundReferenceExpression>(input_types[column_index], column_index));
	}
	return result;
}

} // namespace duckdb
