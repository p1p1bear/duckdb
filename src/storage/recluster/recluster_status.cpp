#include "duckdb/storage/recluster/recluster_status.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <algorithm>

namespace duckdb {

static idx_t AddStatusBytes(idx_t left, idx_t right) {
	return right > NumericLimits<idx_t>::Maximum() - left ? NumericLimits<idx_t>::Maximum() : left + right;
}

class ReclusterStatusBlockCollector : public BlockIdVisitor {
public:
	void Visit(block_id_t block_id) override {
		if (block_id >= 0) {
			blocks.insert(block_id);
		}
	}

	void Add(const MetaBlockPointer &pointer) {
		if (pointer.IsValid()) {
			Visit(pointer.GetBlockId());
		}
	}

	void Add(RowGroup &row_group) {
		for (idx_t column_index = 0; column_index < row_group.GetColumnCount(); column_index++) {
			row_group.GetRawColumnData(column_index).VisitBlockIds(*this);
		}
		for (auto &pointer : row_group.GetColumnStartPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetExtraMetadataBlockPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetDeleteStartPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetLoadedDeleteStoragePointers()) {
			Add(pointer);
		}
	}

	idx_t GetByteSize(const BlockManager &block_manager) const {
		auto block_size = block_manager.GetBlockAllocSize();
		if (block_size != 0 && blocks.size() > NumericLimits<idx_t>::Maximum() / block_size) {
			return NumericLimits<idx_t>::Maximum();
		}
		return blocks.size() * block_size;
	}

private:
	unordered_set<block_id_t> blocks;
};

static idx_t EstimateRowGroupBytes(DataTable &storage, RowGroup &row_group) {
	ReclusterStatusBlockCollector collector;
	collector.Add(row_group);
	auto bytes = collector.GetByteSize(storage.GetTableIOManager().GetBlockManagerForRowData());
	if (bytes > 0) {
		return bytes;
	}
	bytes = row_group.GetAllocationSize();
	if (bytes > 0) {
		return bytes;
	}
	return row_group.count.load();
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

static optional_idx FindSortColumnIndex(const vector<ColumnDefinition> &columns, persistent_column_id_t column_id) {
	for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
		if (columns[column_index].PersistentColumnId() == column_id) {
			return column_index;
		}
	}
	return optional_idx();
}

static string FormatSortColumn(const ColumnList &columns, const SortColumnDefinition &sort_column) {
	for (auto &column : columns.Physical()) {
		if (column.PersistentColumnId() == sort_column.column_id) {
			return SQLIdentifier::ToString(column.Name().GetIdentifierName()) + " ASC NULLS LAST";
		}
	}
	throw InternalException("SORTED BY status references a missing persistent column ID");
}

static bool GetStatisticsRange(RowGroup &row_group, idx_t column_index, Value &minimum, Value &maximum) {
	auto statistics = row_group.GetStatistics(column_index);
	if (!statistics) {
		return false;
	}
	switch (statistics->GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (!NumericStats::HasMinMax(*statistics)) {
			return false;
		}
		minimum = NumericStats::Min(*statistics);
		maximum = NumericStats::Max(*statistics);
		return true;
	case StatisticsType::STRING_STATS:
		if (!StringStats::HasMinMax(*statistics)) {
			return false;
		}
		minimum = Value::BLOB_RAW(StringStats::Min(*statistics));
		maximum = Value::BLOB_RAW(StringStats::Max(*statistics));
		return true;
	default:
		return false;
	}
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
	if (!GetStatisticsRange(row_group, column_index, minimum, maximum)) {
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

static unordered_map<row_t, const RowGroupPhysicalIdentity *>
BuildCheckpointIndex(const optional<CheckpointLayoutSnapshot> &checkpoint) {
	unordered_map<row_t, const RowGroupPhysicalIdentity *> result;
	if (!checkpoint) {
		return result;
	}
	result.reserve(checkpoint->row_groups.size());
	for (auto &identity : checkpoint->row_groups) {
		result.emplace(identity.start, &identity);
	}
	return result;
}

static bool IsCheckpointedRowGroup(RowGroup &row_group, row_t row_start, const vector<ColumnDefinition> &columns,
                                   const unordered_map<row_t, const RowGroupPhysicalIdentity *> &checkpoint_index) {
	auto entry = checkpoint_index.find(row_start);
	if (entry == checkpoint_index.end()) {
		return false;
	}
	auto identity = ComputeRowGroupPhysicalIdentityV1(row_group, row_start, columns);
	return identity && *identity == *entry->second;
}

static ReclusterCandidateLimits StatusCandidateLimits(DataTable &storage) {
	auto row_group_size = storage.GetRowGroupSize();
	auto max_rows =
	    row_group_size > NumericLimits<idx_t>::Maximum() / 32 ? NumericLimits<idx_t>::Maximum() : row_group_size * 32;
	return {max_rows, 32, 4, 0.25};
}

ReclusterTableStatus ReclusterManager::GetTableStatus(DuckTableEntry &table) {
	if (!table.HasSortHistory()) {
		throw InternalException("Cannot build SORTED BY status for a table without sort history");
	}
	auto &storage = table.GetStorage();
	if (&storage.GetAttached() != &db) {
		throw InternalException("Cannot build SORTED BY status through a different attached database");
	}
	auto state = storage.GetDataTableInfo()->GetReclusterState();
	if (!state) {
		state = SynchronizeTable(table);
	}
	if (!state) {
		throw InternalException("SORTED BY status could not initialize table state");
	}
	auto layout_publish_lock = TryGetSharedLayoutPublishLock();
	auto checkpoint_in_progress = !layout_publish_lock;
	layout_publish_lock.reset();

	ReclusterTableStatus result;
	auto &metadata = *table.GetSortMetadata();
	result.table_id = metadata.table_id;
	result.enabled = metadata.IsEnabled();
	result.current_sort_order_id = metadata.current_sort_order_id;
	result.layout_version = storage.GetDataTableInfo()->GetSortStorage().current_layout_version.load();
	result.retired_layout_bytes = retirement_registry.GetRetiredBytes(*storage.GetDataTableInfo());
	result.last_error = state->GetLastError();
	if (!result.enabled) {
		state->ObserveRemainingWorkAgeMs(false);
		return result;
	}

	auto current_definition = metadata.GetCurrent();
	if (!current_definition || current_definition->columns.empty()) {
		throw InternalException("Enabled SORTED BY status has no current definition");
	}
	for (auto &sort_column : current_definition->columns) {
		result.sort_columns.push_back(FormatSortColumn(table.GetColumns(), sort_column));
	}
	auto first_sort_index = FindSortColumnIndex(storage.Columns(), current_definition->columns[0].column_id);
	if (!first_sort_index.IsValid()) {
		throw InternalException("SORTED BY status could not bind its first sort column");
	}

	auto checkpoint = state->GetLastCheckpoint();
	auto checkpoint_index = BuildCheckpointIndex(checkpoint);
	vector<ReclusterRunStatus> runs;
	idx_t total_live_bytes = 0;
	idx_t current_live_bytes = 0;
	LayoutRowGroupCursor cursor(storage.GetRowGroupCollection()->GetCurrentSnapshot());
	LayoutRowGroupEntry entry;
	while (cursor.Next(entry)) {
		auto &row_group = *entry.row_group;
		auto physical_rows = row_group.count.load();
		auto live_rows = row_group.GetCommittedRowCount();
		auto physical_bytes = EstimateRowGroupBytes(storage, row_group);
		auto live_bytes = EstimateLiveBytes(physical_bytes, physical_rows, live_rows);
		total_live_bytes = AddStatusBytes(total_live_bytes, live_bytes);
		auto organization = row_group.GetSortMetadata();
		auto is_current = organization.sort_order_id == result.current_sort_order_id;
		if (is_current) {
			current_live_bytes = AddStatusBytes(current_live_bytes, live_bytes);
			if (runs.empty() || runs.back().run_id != organization.run_id) {
				ReclusterRunStatus run;
				run.run_id = organization.run_id;
				runs.push_back(std::move(run));
			}
			auto &run = runs.back();
			run.live_bytes = AddStatusBytes(run.live_bytes, live_bytes);
			AddRunStatistics(run, row_group, first_sort_index.GetIndex(), live_rows);
		}

		if (!organization.IsSorted()) {
			result.unsorted_bytes = AddStatusBytes(result.unsorted_bytes, physical_bytes);
			auto checkpointed = IsCheckpointedRowGroup(row_group, entry.row_start, storage.Columns(), checkpoint_index);
			if (!checkpointed) {
				result.not_checkpointed_unsorted_bytes =
				    AddStatusBytes(result.not_checkpointed_unsorted_bytes, physical_bytes);
			}
			if (row_group.IsSealed() || checkpointed || physical_rows >= storage.GetRowGroupSize()) {
				result.unsorted_row_groups++;
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
	result.remaining_recluster_bytes = EstimateRemainingReclusterBytes(storage, *state);
	result.oldest_unsorted_age_ms = state->ObserveRemainingWorkAgeMs(result.unsorted_row_groups > 0);

	auto task_status = state->GetTaskStatus();
	result.active_prepare_tasks = task_status.active_prepare_tasks;
	result.pending_finalize_tasks = task_status.pending_finalize_tasks;
	result.pending_delete_rows = task_status.pending_delete_rows;
	result.prepared_bytes = task_status.prepared_bytes;
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
	auto layout = storage.GetRowGroupCollection()->GetCurrentLayout();
	if (layout && layout->patches.size() >= MAX_LAYOUT_PATCHES_PER_CHECKPOINT) {
		result.blocked_reason = "LAYOUT_PATCH_LIMIT";
		return result;
	}
	if (!checkpoint || checkpoint->checkpoint_number == 0) {
		result.blocked_reason = "NO_CHECKPOINTED_RANGE";
		return result;
	}
	auto selection = SelectReclusterCandidate(*storage.GetRowGroupCollection(), storage.Columns(), *state,
	                                          StatusCandidateLimits(storage));
	if (selection.status == ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT) {
		result.blocked_reason = "RUN_EXCEEDS_TASK_LIMIT";
	} else if (!selection.candidate) {
		result.blocked_reason = "NO_CHECKPOINTED_RANGE";
	}
	return result;
}

} // namespace duckdb
