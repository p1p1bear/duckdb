//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/table_sort_bind.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/recluster/table_sort_metadata.hpp"

namespace duckdb {
class ColumnList;
struct OrderByNode;

SortOrderDefinition BindPersistentSortDefinition(const vector<OrderByNode> &orders, const ColumnList &columns,
                                                 sort_order_id_t sort_order_id);
void ValidateTableSortCatalogMetadata(const TableSortCatalogMetadata &metadata, const ColumnList &columns);

} // namespace duckdb
