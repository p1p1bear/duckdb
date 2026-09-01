//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_block_metrics.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class BlockManager;
class RowGroup;

bool AddReclusterRowGroupBlocks(RowGroup &row_group, unordered_set<block_id_t> &blocks);
idx_t GetReclusterRowGroupTransientBytes(RowGroup &row_group, idx_t physical_rows, bool has_persistent_blocks);
idx_t GetReclusterBlockBytes(const BlockManager &block_manager, const unordered_set<block_id_t> &blocks);

} // namespace duckdb
