#include "duckdb/storage/recluster/table_sort_metadata.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/storage/storage_lock.hpp"

namespace duckdb {

void SortColumnDefinition::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<persistent_column_id_t>(100, "column_id", column_id);
	serializer.WriteProperty<OrderType>(101, "order_type", order_type);
	serializer.WriteProperty<OrderByNullType>(102, "null_order", null_order);
}

SortColumnDefinition SortColumnDefinition::Deserialize(Deserializer &deserializer) {
	SortColumnDefinition result;
	deserializer.ReadProperty<persistent_column_id_t>(100, "column_id", result.column_id);
	deserializer.ReadProperty<OrderType>(101, "order_type", result.order_type);
	deserializer.ReadProperty<OrderByNullType>(102, "null_order", result.null_order);
	return result;
}

bool SortColumnDefinition::operator==(const SortColumnDefinition &other) const {
	return column_id == other.column_id && order_type == other.order_type && null_order == other.null_order;
}

void SortOrderDefinition::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<sort_order_id_t>(100, "sort_order_id", sort_order_id);
	serializer.WriteProperty<vector<SortColumnDefinition>>(101, "columns", columns);
}

SortOrderDefinition SortOrderDefinition::Deserialize(Deserializer &deserializer) {
	SortOrderDefinition result;
	deserializer.ReadProperty<sort_order_id_t>(100, "sort_order_id", result.sort_order_id);
	deserializer.ReadProperty<vector<SortColumnDefinition>>(101, "columns", result.columns);
	return result;
}

bool SortOrderDefinition::operator==(const SortOrderDefinition &other) const {
	return sort_order_id == other.sort_order_id && columns == other.columns;
}

const SortOrderDefinition *TableSortCatalogMetadata::GetCurrent() const {
	return GetDefinition(current_sort_order_id);
}

const SortOrderDefinition *TableSortCatalogMetadata::GetDefinition(sort_order_id_t id) const {
	if (id == INVALID_SORT_ORDER_ID) {
		return nullptr;
	}
	for (auto &definition : definitions) {
		if (definition.sort_order_id == id) {
			return &definition;
		}
	}
	return nullptr;
}

bool TableSortCatalogMetadata::IsEnabled() const {
	return current_sort_order_id != INVALID_SORT_ORDER_ID;
}

void TableSortCatalogMetadata::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<persistent_table_id_t>(100, "table_id", table_id);
	serializer.WriteProperty<persistent_column_id_t>(101, "next_column_id", next_column_id);
	serializer.WriteProperty<sort_order_id_t>(102, "current_sort_order_id", current_sort_order_id);
	serializer.WriteProperty<sort_order_id_t>(103, "next_sort_order_id", next_sort_order_id);
	serializer.WriteProperty<vector<SortOrderDefinition>>(104, "definitions", definitions);
}

TableSortCatalogMetadata TableSortCatalogMetadata::Deserialize(Deserializer &deserializer) {
	TableSortCatalogMetadata result;
	deserializer.ReadProperty<persistent_table_id_t>(100, "table_id", result.table_id);
	deserializer.ReadProperty<persistent_column_id_t>(101, "next_column_id", result.next_column_id);
	deserializer.ReadProperty<sort_order_id_t>(102, "current_sort_order_id", result.current_sort_order_id);
	deserializer.ReadProperty<sort_order_id_t>(103, "next_sort_order_id", result.next_sort_order_id);
	deserializer.ReadProperty<vector<SortOrderDefinition>>(104, "definitions", result.definitions);
	return result;
}

bool TableSortCatalogMetadata::operator==(const TableSortCatalogMetadata &other) const {
	return table_id == other.table_id && next_column_id == other.next_column_id &&
	       current_sort_order_id == other.current_sort_order_id && next_sort_order_id == other.next_sort_order_id &&
	       definitions == other.definitions;
}

bool PersistentTableSortStorageMetadata::operator==(const PersistentTableSortStorageMetadata &other) const {
	return next_run_id == other.next_run_id && current_layout_version == other.current_layout_version;
}

TableSortStorageState::TableSortStorageState(const PersistentTableSortStorageMetadata &metadata)
    : next_run_id(metadata.next_run_id), current_layout_version(metadata.current_layout_version) {
	if (metadata.next_run_id == INVALID_SORT_RUN_ID) {
		throw SerializationException("SORTED BY storage state has an invalid next run ID");
	}
}

PersistentTableSortStorageMetadata
TableSortStorageState::GetPersistentSnapshot(const StorageLockKey &checkpoint_or_finalize_lock) const {
	(void)checkpoint_or_finalize_lock;
	auto result = PersistentTableSortStorageMetadata {next_run_id.load(), current_layout_version.load()};
	D_ASSERT(result.next_run_id != INVALID_SORT_RUN_ID);
	return result;
}

void PersistentColumnIdAssignment::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<idx_t>(100, "logical_column_index", logical_column_index);
	serializer.WriteProperty<string>(101, "name", name);
	serializer.WriteProperty<LogicalType>(102, "type", type);
	serializer.WriteProperty<persistent_column_id_t>(103, "column_id", column_id);
}

PersistentColumnIdAssignment PersistentColumnIdAssignment::Deserialize(Deserializer &deserializer) {
	PersistentColumnIdAssignment result;
	deserializer.ReadProperty<idx_t>(100, "logical_column_index", result.logical_column_index);
	deserializer.ReadProperty<string>(101, "name", result.name);
	deserializer.ReadProperty<LogicalType>(102, "type", result.type);
	deserializer.ReadProperty<persistent_column_id_t>(103, "column_id", result.column_id);
	return result;
}

bool PersistentColumnIdAssignment::operator==(const PersistentColumnIdAssignment &other) const {
	return logical_column_index == other.logical_column_index && name == other.name && type == other.type &&
	       column_id == other.column_id;
}

void TableSortCatalogPostImage::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<TableSortCatalogMetadata>(100, "table_metadata", table_metadata);
	serializer.WriteProperty<vector<PersistentColumnIdAssignment>>(101, "columns", columns);
}

TableSortCatalogPostImage TableSortCatalogPostImage::Deserialize(Deserializer &deserializer) {
	TableSortCatalogPostImage result;
	deserializer.ReadProperty<TableSortCatalogMetadata>(100, "table_metadata", result.table_metadata);
	deserializer.ReadProperty<vector<PersistentColumnIdAssignment>>(101, "columns", result.columns);
	return result;
}

bool TableSortCatalogPostImage::operator==(const TableSortCatalogPostImage &other) const {
	return table_metadata == other.table_metadata && columns == other.columns;
}

} // namespace duckdb
