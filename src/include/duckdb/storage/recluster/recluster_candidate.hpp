//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_candidate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class RowGroupCollection;
class TableReclusterState;

enum class ReclusterCandidateType : uint8_t { CONVERSION, DELETE_CLEANUP, RUN_MERGE };

enum class ReclusterCandidateSelectionStatus : uint8_t {
	SELECTED,
	NO_CHECKPOINTED_RANGE,
	NO_ELIGIBLE_RANGE,
	RUN_EXCEEDS_TASK_LIMIT
};

struct ReclusterCandidateLimits {
	idx_t max_physical_rows = 0;
	idx_t max_row_groups = 0;
	idx_t max_merge_runs = 0;
	double delete_cleanup_ratio = 0;
};

struct ReclusterCandidate {
	ReclusterCandidateType type = ReclusterCandidateType::CONVERSION;
	RowGroupRange range {0, 0};
	uint64_t checkpoint_number = 0;
	uint64_t storage_generation_id = 0;
	layout_version_t layout_version = INITIAL_LAYOUT_VERSION;
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	idx_t input_physical_rows = 0;
	idx_t input_live_rows = 0;
	idx_t input_deleted_rows = 0;
	idx_t row_group_count = 0;
	idx_t run_count = 0;
	vector<RowGroupPhysicalIdentity> expected_row_groups;
};

struct ReclusterCandidateSelection {
	ReclusterCandidateSelectionStatus status = ReclusterCandidateSelectionStatus::NO_CHECKPOINTED_RANGE;
	optional<ReclusterCandidate> candidate;
};

ReclusterCandidateSelection SelectReclusterCandidate(RowGroupCollection &collection,
                                                     const vector<ColumnDefinition> &columns,
                                                     TableReclusterState &state,
                                                     const ReclusterCandidateLimits &limits);

} // namespace duckdb
