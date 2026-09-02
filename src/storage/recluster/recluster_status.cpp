#include "duckdb/storage/recluster/recluster_status.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_block_metrics.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/recluster/table_sort_bind.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <algorithm>

namespace duckdb {

static idx_t EstimateRowGroupBytes(DataTable &storage, idx_t transient_bytes, const unordered_set<block_id_t> &blocks) {
	auto persistent_bytes = GetReclusterBlockBytes(storage.GetTableIOManager().GetBlockManagerForRowData(), blocks);
	return SaturatingAddReclusterValue(persistent_bytes, transient_bytes);
}

static idx_t EstimateLiveBytes(idx_t bytes, idx_t physical_rows, idx_t live_rows) {
	if (bytes == 0 || physical_rows == 0 || live_rows == 0) {
		return 0;
	}
	if (live_rows >= physical_rows) {
		return bytes;
	}
	auto scaled = static_cast<idx_t>((static_cast<long double>(bytes) * static_cast<long double>(live_rows)) /
	                                 static_cast<long double>(physical_rows));
	return MaxValue<idx_t>(scaled, 1);
}

struct ReclusterRunStatus {
	sort_run_id_t run_id = INVALID_SORT_RUN_ID;
	idx_t live_bytes = 0;
	bool statistics_known = true;
	bool has_statistics = false;
	Value minimum;
	Value maximum;
};

static void AddRunStatistics(ReclusterRunStatus &run, RowGroup &row_group, idx_t column_index, idx_t live_rows) {
	if (live_rows == 0 || !run.statistics_known) {
		return;
	}
	Value minimum;
	Value maximum;
	if (!GetReclusterRowGroupStatisticsRange(row_group, column_index, minimum, maximum)) {
		run.statistics_known = false;
		return;
	}
	if (!run.has_statistics) {
		run.minimum = std::move(minimum);
		run.maximum = std::move(maximum);
		run.has_statistics = true;
		return;
	}
	if (ValueOperations::DistinctLessThan(minimum, run.minimum)) {
		run.minimum = std::move(minimum);
	}
	if (ValueOperations::DistinctGreaterThan(maximum, run.maximum)) {
		run.maximum = std::move(maximum);
	}
}

struct ReclusterOverlapEvent {
	Value value;
	bool starts;
};

static void ComputeOverlapDepth(const vector<ReclusterRunStatus> &runs, idx_t &maximum, idx_t &p95) {
	idx_t unknown_ranges = 0;
	vector<ReclusterOverlapEvent> events;
	events.reserve(runs.size() * 2);
	for (auto &run : runs) {
		if (!run.statistics_known || !run.has_statistics) {
			unknown_ranges++;
			continue;
		}
		events.push_back({run.minimum, true});
		events.push_back({run.maximum, false});
	}
	std::sort(events.begin(), events.end(), [](const ReclusterOverlapEvent &left, const ReclusterOverlapEvent &right) {
		if (ValueOperations::NotDistinctFrom(left.value, right.value)) {
			return left.starts && !right.starts;
		}
		return ValueOperations::DistinctLessThan(left.value, right.value);
	});

	idx_t depth = unknown_ranges;
	vector<idx_t> observed_depths;
	idx_t event_index = 0;
	while (event_index < events.size()) {
		auto event_end = event_index + 1;
		while (event_end < events.size() &&
		       ValueOperations::NotDistinctFrom(events[event_index].value, events[event_end].value)) {
			event_end++;
		}
		idx_t starts = 0;
		idx_t ends = 0;
		for (idx_t index = event_index; index < event_end; index++) {
			if (events[index].starts) {
				starts++;
			} else {
				ends++;
			}
		}
		depth += starts;
		maximum = MaxValue(maximum, depth);
		for (idx_t index = event_index; index < event_end; index++) {
			observed_depths.push_back(depth);
		}
		depth -= ends;
		event_index = event_end;
	}
	if (events.empty() && unknown_ranges > 0) {
		maximum = unknown_ranges;
		observed_depths.push_back(unknown_ranges);
	}
	if (observed_depths.empty()) {
		return;
	}
	std::sort(observed_depths.begin(), observed_depths.end());
	auto rank = observed_depths.size() - observed_depths.size() / 20;
	p95 = observed_depths[rank - 1];
}

static ReclusterCandidateLimits StatusCandidateLimits(DataTable &storage) {
	return GetReclusterCandidateLimits(storage, GetReclusterRowGroupLimit(storage));
}

ReclusterTableStatus ReclusterManager::GetTableStatus(DuckTableEntry &table) {
	if (!table.HasSortHistory()) {
		throw InternalException("Cannot build SORTED BY status for a table without sort history");
	}
	auto &storage = table.GetStorage();
	if (&storage.GetAttached() != &db) {
		throw InternalException("Cannot build SORTED BY status through a different attached database");
	}
	auto layout_publish_lock = TryGetSharedLayoutPublishLock();
	auto checkpoint_in_progress = !layout_publish_lock;
	layout_publish_lock.reset();

	ReclusterTableStatus result;
	auto &metadata = *table.GetSortMetadata();
	auto &table_info = *storage.GetDataTableInfo();
	auto row_groups = storage.GetRowGroupCollection();
	auto storage_generation_id = row_groups->GetStorageGenerationId();
	auto shared_state = table_info.GetReclusterState();
	TableReclusterSchedulingSnapshot scheduling;
	if (shared_state) {
		scheduling = shared_state->GetSchedulingSnapshot();
	}
	auto state_matches_catalog = shared_state && scheduling.table_id == metadata.table_id &&
	                             scheduling.sort_order_id == metadata.current_sort_order_id &&
	                             scheduling.storage_generation_id == storage_generation_id;
	if (!state_matches_catalog && table.timestamp.load() < TRANSACTION_ID_START) {
		auto ddl_coordination_lock = table_info.GetReclusterDDLCoordinationLock();
		if (storage.IsMainTable()) {
			shared_state = SynchronizeTable(table);
			D_ASSERT(shared_state);
			scheduling = shared_state->GetSchedulingSnapshot();
			state_matches_catalog = scheduling.table_id == metadata.table_id &&
			                        scheduling.sort_order_id == metadata.current_sort_order_id &&
			                        scheduling.storage_generation_id == storage_generation_id;
		}
	}
	result.table_id = metadata.table_id;
	result.enabled = metadata.IsEnabled();
	result.current_sort_order_id = metadata.current_sort_order_id;
	result.layout_version = table_info.GetSortStorage().current_layout_version.load();
	result.retired_layout_bytes = retirement_registry.GetRetiredBytes(table_info);
	if (!result.enabled) {
		if (shared_state) {
			result.last_error = shared_state->GetLastError();
			shared_state->ObserveRemainingWorkAgeMsIfMatches(metadata.table_id, metadata.current_sort_order_id,
			                                                 storage_generation_id, false);
		}
		return result;
	}

	auto current_definition = metadata.GetCurrent();
	if (!current_definition || current_definition->columns.empty()) {
		throw InternalException("Enabled SORTED BY status has no current definition");
	}
	auto physical_sort_indexes = BindPersistentSortIndexes(storage.Columns(), *current_definition);
	for (auto physical_index : physical_sort_indexes) {
		auto &column = table.GetColumns().GetColumn(PhysicalIndex(physical_index));
		result.sort_columns.push_back(SQLIdentifier::ToString(column.Name().GetIdentifierName()) + " ASC NULLS LAST");
	}
	auto first_sort_index = optional_idx(physical_sort_indexes[0]);

	if (!state_matches_catalog) {
		scheduling = TableReclusterSchedulingSnapshot();
		scheduling.table_id = metadata.table_id;
		scheduling.sort_order_id = metadata.current_sort_order_id;
		scheduling.storage_generation_id = storage_generation_id;
	}
	ReclusterLayoutAnalysis analysis(*row_groups, storage.Columns(), std::move(scheduling), first_sort_index);
	if (state_matches_catalog) {
		result.last_error = shared_state->GetLastError();
	}
	result.layout_version = analysis.GetLayoutVersion();
	vector<ReclusterRunStatus> runs;
	idx_t total_live_bytes = 0;
	idx_t current_live_bytes = 0;
	bool previous_was_current = false;
	bool has_remaining_work = false;
	idx_t transient_remaining_bytes = 0;
	unordered_set<block_id_t> remaining_blocks;
	unordered_set<block_id_t> row_group_blocks;
	for (idx_t row_group_index = 0; row_group_index < analysis.GetRowGroups().size(); row_group_index++) {
		auto &row_group_analysis = analysis.GetRowGroups()[row_group_index];
		auto &row_group = *row_group_analysis.entry.row_group;
		row_group_blocks.clear();
		auto has_persistent_blocks = AddReclusterRowGroupBlocks(row_group, row_group_blocks);
		auto physical_rows = row_group_analysis.physical_rows;
		auto live_rows = row_group_analysis.live_rows;
		auto transient_bytes = GetReclusterRowGroupTransientBytes(row_group, physical_rows, has_persistent_blocks);
		auto physical_bytes = EstimateRowGroupBytes(storage, transient_bytes, row_group_blocks);
		auto live_bytes = EstimateLiveBytes(physical_bytes, physical_rows, live_rows);
		total_live_bytes = SaturatingAddReclusterValue(total_live_bytes, live_bytes);
		auto organization = row_group_analysis.sort_metadata;
		auto is_current = organization.sort_order_id == result.current_sort_order_id;
		if (is_current) {
			current_live_bytes = SaturatingAddReclusterValue(current_live_bytes, live_bytes);
			if (!previous_was_current || runs.empty() || runs.back().run_id != organization.run_id) {
				ReclusterRunStatus run;
				run.run_id = organization.run_id;
				runs.push_back(std::move(run));
			}
			auto &run = runs.back();
			run.live_bytes = SaturatingAddReclusterValue(run.live_bytes, live_bytes);
			AddRunStatistics(run, row_group, first_sort_index.GetIndex(), live_rows);
		}

		previous_was_current = is_current;
		if (!organization.IsSorted()) {
			result.unsorted_bytes = SaturatingAddReclusterValue(result.unsorted_bytes, physical_bytes);
			auto checkpointed = analysis.IsCheckpointedRowGroup(row_group_index);
			if (!checkpointed) {
				result.not_checkpointed_unsorted_bytes =
				    SaturatingAddReclusterValue(result.not_checkpointed_unsorted_bytes, physical_bytes);
			}
			if (row_group.IsSealed() || checkpointed || physical_rows >= storage.GetRowGroupSize()) {
				result.unsorted_row_groups++;
			}
		}
		if (analysis.RequiresRewrite(row_group_analysis)) {
			has_remaining_work = true;
			transient_remaining_bytes = SaturatingAddReclusterValue(transient_remaining_bytes, transient_bytes);
			if (has_persistent_blocks) {
				remaining_blocks.insert(row_group_blocks.begin(), row_group_blocks.end());
			}
		}
	}

	result.run_count = runs.size();
	if (total_live_bytes == 0) {
		result.current_order_coverage = 1;
	} else {
		result.current_order_coverage = static_cast<double>(current_live_bytes) / static_cast<double>(total_live_bytes);
	}
	idx_t largest_run_bytes = 0;
	for (auto &run : runs) {
		largest_run_bytes = MaxValue(largest_run_bytes, run.live_bytes);
	}
	if (total_live_bytes > 0) {
		result.largest_run_fraction = static_cast<double>(largest_run_bytes) / static_cast<double>(total_live_bytes);
	}
	ComputeOverlapDepth(runs, result.max_overlap_depth, result.p95_overlap_depth);
	auto persistent_remaining_bytes =
	    GetReclusterBlockBytes(storage.GetTableIOManager().GetBlockManagerForRowData(), remaining_blocks);
	result.remaining_recluster_bytes =
	    SaturatingAddReclusterValue(persistent_remaining_bytes, transient_remaining_bytes);
	if (has_remaining_work && result.remaining_recluster_bytes == 0) {
		result.remaining_recluster_bytes = 1;
	}
	if (state_matches_catalog) {
		result.oldest_unsorted_age_ms = shared_state->ObserveRemainingWorkAgeMsIfMatches(
		    metadata.table_id, metadata.current_sort_order_id, storage_generation_id, result.unsorted_row_groups > 0);
	}

	if (state_matches_catalog) {
		auto task_status = shared_state->GetTaskStatus();
		result.active_prepare_tasks = task_status.active_prepare_tasks;
		result.pending_finalize_tasks = task_status.pending_finalize_tasks;
		result.pending_delete_rows = task_status.pending_delete_rows;
		result.prepared_bytes = task_status.prepared_bytes;
	}
	if (result.pending_finalize_tasks > 0 && result.pending_delete_rows > 0) {
		result.blocked_reason = "FINAL_DELETE_BACKLOG";
		return result;
	}
	if (result.pending_finalize_tasks > 0 && checkpoint_in_progress) {
		result.blocked_reason = "CHECKPOINT_IN_PROGRESS";
		return result;
	}
	if (result.remaining_recluster_bytes == 0 || result.active_prepare_tasks > 0 || result.pending_finalize_tasks > 0) {
		return result;
	}
	if (analysis.GetLayoutPatchCount() >= MAX_LAYOUT_PATCHES_PER_CHECKPOINT) {
		result.blocked_reason = "LAYOUT_PATCH_LIMIT";
		return result;
	}
	if (!analysis.HasUsableCheckpoint()) {
		result.blocked_reason = "NO_CHECKPOINTED_RANGE";
		return result;
	}
	auto selection = analysis.SelectCandidate(StatusCandidateLimits(storage));
	if (selection.status == ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT) {
		result.blocked_reason = "RUN_EXCEEDS_TASK_LIMIT";
	} else if (!selection.candidate) {
		result.blocked_reason = "NO_CHECKPOINTED_RANGE";
	}
	return result;
}

} // namespace duckdb
