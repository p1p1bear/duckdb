//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_status.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

struct ReclusterTableStatus {
	persistent_table_id_t table_id = hugeint_t(0, 0);
	bool enabled = false;
	sort_order_id_t current_sort_order_id = INVALID_SORT_ORDER_ID;
	vector<string> sort_columns;
	layout_version_t layout_version = INITIAL_LAYOUT_VERSION;
	double current_order_coverage = 0;
	idx_t unsorted_row_groups = 0;
	idx_t unsorted_bytes = 0;
	idx_t not_checkpointed_unsorted_bytes = 0;
	optional<int64_t> oldest_unsorted_age_ms;
	idx_t run_count = 0;
	idx_t max_overlap_depth = 0;
	idx_t p95_overlap_depth = 0;
	idx_t remaining_recluster_bytes = 0;
	double largest_run_fraction = 0;
	idx_t active_prepare_tasks = 0;
	idx_t pending_finalize_tasks = 0;
	idx_t pending_delete_rows = 0;
	idx_t prepared_bytes = 0;
	idx_t retired_layout_bytes = 0;
	optional<string> blocked_reason;
	optional<string> last_error;
};

} // namespace duckdb
