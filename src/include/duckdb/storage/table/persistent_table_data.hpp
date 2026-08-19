//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/persistent_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {
class BaseStatistics;

class PersistentTableData {
public:
	explicit PersistentTableData(idx_t column_count);
	~PersistentTableData();

	MetaBlockPointer base_table_pointer;
	vector<MetaBlockPointer> read_metadata_pointers;
	TableStatistics table_stats;
	idx_t total_rows;
	idx_t next_row_id;
	idx_t row_group_count;
	MetaBlockPointer block_pointer;
	optional<PersistentTableSortStorageMetadata> sort_storage_metadata;
};

} // namespace duckdb
