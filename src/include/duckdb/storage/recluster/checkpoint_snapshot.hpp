//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/checkpoint_snapshot.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

namespace duckdb {

class RowGroup;
class RowGroupCollection;

struct RowGroupColumnPhysicalIdentity {
	persistent_column_id_t column_id = 0;
	LogicalType type;
	MetaBlockPointer column_data_pointer;
	vector<MetaBlockPointer> additional_metadata_pointers;

	bool operator==(const RowGroupColumnPhysicalIdentity &other) const;
};

struct RowGroupPhysicalIdentity {
	uint32_t format_version = 1;
	row_t start = 0;
	idx_t count = 0;
	bool sealed = false;
	vector<RowGroupColumnPhysicalIdentity> columns;
	RowGroupSortMetadata sort_metadata;
	uint64_t immutable_data_checksum = 0;

	bool operator==(const RowGroupPhysicalIdentity &other) const;
};

struct CheckpointLayoutSnapshot {
	uint64_t checkpoint_number = 0;
	uint64_t storage_generation_id = 0;
	vector<RowGroupPhysicalIdentity> row_groups;

	bool operator==(const CheckpointLayoutSnapshot &other) const;
};

uint64_t ComputeRowGroupPhysicalIdentityChecksumV1(const RowGroupPhysicalIdentity &identity);
optional<RowGroupPhysicalIdentity> ComputeRowGroupPhysicalIdentityV1(const RowGroup &row_group, row_t row_start,
                                                                     const vector<ColumnDefinition> &columns);
optional<CheckpointLayoutSnapshot> BuildCheckpointLayoutSnapshot(RowGroupCollection &collection,
                                                                 const vector<ColumnDefinition> &columns,
                                                                 uint64_t checkpoint_number);

} // namespace duckdb
