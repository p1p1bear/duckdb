//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/table_sort_metadata.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

#include <atomic>

namespace duckdb {
class Deserializer;
class Serializer;
class StorageLockKey;

struct SortColumnDefinition {
	persistent_column_id_t column_id = 0;
	OrderType order_type = OrderType::ASCENDING;
	OrderByNullType null_order = OrderByNullType::NULLS_LAST;

	void Serialize(Serializer &serializer) const;
	static SortColumnDefinition Deserialize(Deserializer &deserializer);

	bool operator==(const SortColumnDefinition &other) const;
};

struct SortOrderDefinition {
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	vector<SortColumnDefinition> columns;

	void Serialize(Serializer &serializer) const;
	static SortOrderDefinition Deserialize(Deserializer &deserializer);

	bool operator==(const SortOrderDefinition &other) const;
};

struct TableSortCatalogMetadata {
	persistent_table_id_t table_id = hugeint_t(0, 0);
	persistent_column_id_t next_column_id = 1;
	sort_order_id_t current_sort_order_id = INVALID_SORT_ORDER_ID;
	sort_order_id_t next_sort_order_id = 1;
	vector<SortOrderDefinition> definitions;

	const SortOrderDefinition *GetCurrent() const;
	const SortOrderDefinition *GetDefinition(sort_order_id_t id) const;
	bool IsEnabled() const;

	void Serialize(Serializer &serializer) const;
	static TableSortCatalogMetadata Deserialize(Deserializer &deserializer);

	bool operator==(const TableSortCatalogMetadata &other) const;
};

struct PersistentTableSortStorageMetadata {
	sort_run_id_t next_run_id = 1;
	layout_version_t current_layout_version = INITIAL_LAYOUT_VERSION;

	bool operator==(const PersistentTableSortStorageMetadata &other) const;
};

struct TableSortStorageState {
	explicit TableSortStorageState(const PersistentTableSortStorageMetadata &metadata);

	sort_run_id_t AllocateRunId();
	PersistentTableSortStorageMetadata GetPersistentSnapshot(const StorageLockKey &checkpoint_or_finalize_lock) const;

	std::atomic<sort_run_id_t> next_run_id;
	std::atomic<layout_version_t> current_layout_version;
};

struct PersistentColumnIdAssignment {
	idx_t logical_column_index = DConstants::INVALID_INDEX;
	string name;
	LogicalType type;
	persistent_column_id_t column_id = 0;

	void Serialize(Serializer &serializer) const;
	static PersistentColumnIdAssignment Deserialize(Deserializer &deserializer);

	bool operator==(const PersistentColumnIdAssignment &other) const;
};

struct TableSortCatalogPostImage {
	TableSortCatalogMetadata table_metadata;
	vector<PersistentColumnIdAssignment> columns;

	void Serialize(Serializer &serializer) const;
	static TableSortCatalogPostImage Deserialize(Deserializer &deserializer);

	bool operator==(const TableSortCatalogPostImage &other) const;
};

struct RowGroupSortMetadata {
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	sort_run_id_t run_id = INVALID_SORT_RUN_ID;

	bool IsSorted() const {
		return sort_order_id != INVALID_SORT_ORDER_ID && run_id != INVALID_SORT_RUN_ID;
	}

	bool IsValid() const {
		return (sort_order_id == INVALID_SORT_ORDER_ID) == (run_id == INVALID_SORT_RUN_ID);
	}

	bool operator==(const RowGroupSortMetadata &other) const {
		return sort_order_id == other.sort_order_id && run_id == other.run_id;
	}

	bool operator!=(const RowGroupSortMetadata &other) const {
		return !(*this == other);
	}
};

} // namespace duckdb
