//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_candidate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class DataTable;
class RowGroupCollection;
class RowGroup;
class TableReclusterState;
class Value;
struct TableReclusterSchedulingSnapshot;

static constexpr idx_t DEFAULT_RECLUSTER_MAX_MERGE_RUNS = 4;
static constexpr idx_t FULL_RECLUSTER_MAX_MERGE_RUNS = 32;

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
	bool prioritize_overlap = true;
};

idx_t GetReclusterRowGroupLimit(DataTable &storage);
idx_t GetFullReclusterRowGroupLimit(DataTable &storage);
idx_t GetReclusterTaskInputByteLimit(DataTable &storage);
ReclusterCandidateLimits GetReclusterCandidateLimits(DataTable &storage, idx_t max_row_groups,
                                                     idx_t max_merge_runs = DEFAULT_RECLUSTER_MAX_MERGE_RUNS);

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

struct ReclusterAnalyzedRowGroup {
	LayoutRowGroupEntry entry;
	RowGroupSortMetadata sort_metadata;
	idx_t physical_rows = 0;
	idx_t live_rows = 0;
};

class ReclusterLayoutAnalysisState;

class ReclusterLayoutAnalysis {
public:
	ReclusterLayoutAnalysis(RowGroupCollection &collection, const vector<ColumnDefinition> &columns,
	                        TableReclusterState &state, optional_idx first_sort_column = optional_idx());
	ReclusterLayoutAnalysis(RowGroupCollection &collection, const vector<ColumnDefinition> &columns,
	                        TableReclusterSchedulingSnapshot scheduling,
	                        optional_idx first_sort_column = optional_idx());
	~ReclusterLayoutAnalysis();

	ReclusterCandidateSelection SelectCandidate(const ReclusterCandidateLimits &limits);
	idx_t GetCheckpointRowGroupCount() const;
	bool HasUsableCheckpoint() const;
	const vector<ReclusterAnalyzedRowGroup> &GetRowGroups() const;
	bool IsCheckpointedRowGroup(idx_t row_group_index);
	layout_version_t GetLayoutVersion() const;
	idx_t GetLayoutPatchCount() const;
	bool RequiresRewrite(const ReclusterAnalyzedRowGroup &row_group) const;

private:
	unique_ptr<ReclusterLayoutAnalysisState> analysis;
};

optional<ReclusterCandidate> RevalidateReclusterCandidate(RowGroupCollection &collection,
                                                          const vector<ColumnDefinition> &columns,
                                                          TableReclusterState &state,
                                                          const ReclusterCandidate &candidate);
bool GetReclusterRowGroupStatisticsRange(RowGroup &row_group, idx_t column_index, Value &minimum, Value &maximum);

} // namespace duckdb
