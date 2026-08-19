//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/hugeint.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

using persistent_column_id_t = uint64_t;
using sort_order_id_t = uint64_t;
using sort_run_id_t = uint64_t;
using layout_version_t = uint64_t;
using delete_sequence_t = uint64_t;

using persistent_table_id_t = hugeint_t;
using recluster_task_id_t = hugeint_t;
using retirement_id_t = uint64_t;

static constexpr sort_order_id_t INVALID_SORT_ORDER_ID = 0;
static constexpr sort_run_id_t INVALID_SORT_RUN_ID = 0;
static constexpr layout_version_t INITIAL_LAYOUT_VERSION = 0;
static constexpr StorageVersion MIN_SORTED_BY_STORAGE_VERSION = StorageVersion::V2_0_0;

} // namespace duckdb
