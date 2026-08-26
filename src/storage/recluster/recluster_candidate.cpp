#include "duckdb/storage/recluster/recluster_candidate.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <cmath>

namespace duckdb {

static constexpr idx_t RECLUSTER_REMAP_MEMORY_DIVISOR = 64;
static constexpr idx_t RECLUSTER_INPUT_MEMORY_DIVISOR = 8;

idx_t GetReclusterRowGroupLimit(DataTable &storage) {
	auto row_group_size = storage.GetRowGroupSize();
	if (row_group_size == 0) {
		throw InternalException("Recluster candidate requires a non-zero row group size");
	}
	auto max_memory = BufferManager::GetBufferManager(storage.GetAttached()).GetMaxMemory();
	auto remap_budget = max_memory / RECLUSTER_REMAP_MEMORY_DIVISOR;
	auto minimum_rows =
	    row_group_size > NumericLimits<idx_t>::Maximum() / 2 ? NumericLimits<idx_t>::Maximum() : row_group_size * 2;
	auto minimum_budget = minimum_rows > NumericLimits<idx_t>::Maximum() / sizeof(row_t)
	                          ? NumericLimits<idx_t>::Maximum()
	                          : minimum_rows * sizeof(row_t);
	remap_budget = MaxValue(remap_budget, minimum_budget);
	auto max_rows = remap_budget / sizeof(row_t);
	return MaxValue<idx_t>(max_rows / row_group_size, 2);
}

idx_t GetReclusterTaskInputByteLimit(DataTable &storage) {
	auto max_memory = BufferManager::GetBufferManager(storage.GetAttached()).GetMaxMemory();
	auto block_size = storage.GetTableIOManager().GetBlockManagerForRowData().GetBlockAllocSize();
	return MaxValue(max_memory / RECLUSTER_INPUT_MEMORY_DIVISOR, block_size);
}

ReclusterCandidateLimits GetReclusterCandidateLimits(DataTable &storage, idx_t max_row_groups) {
	if (max_row_groups == 0) {
		throw InternalException("Recluster candidate requires a non-zero row group limit");
	}
	auto row_group_size = storage.GetRowGroupSize();
	auto max_rows = row_group_size > NumericLimits<idx_t>::Maximum() / max_row_groups ? NumericLimits<idx_t>::Maximum()
	                                                                                  : row_group_size * max_row_groups;
	return {max_rows, max_row_groups, 4, 0.25};
}

struct CandidateRowGroupState {
	bool available = false;
	idx_t live_rows = 0;
};

struct CandidateUnit {
	idx_t input_begin = 0;
	idx_t input_end = 0;
	RowGroupRange range {0, 0};
	bool current_run = false;
	bool available = false;
	idx_t physical_rows = 0;
	idx_t live_rows = 0;
	idx_t deleted_rows = 0;
	idx_t row_group_count = 0;
};

static RowGroupRange GetIdentityRange(const RowGroupPhysicalIdentity &identity) {
	if (identity.start < 0 || identity.count == 0 ||
	    identity.count > static_cast<idx_t>(NumericLimits<row_t>::Maximum() - identity.start)) {
		throw InternalException("Checkpoint row group identity has an invalid row ID range");
	}
	return {identity.start, identity.start + NumericCast<row_t>(identity.count)};
}

static idx_t AddCount(idx_t left, idx_t right) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InternalException("Recluster candidate row count overflow");
	}
	return left + right;
}

static bool OverlapsAny(const RowGroupRange &range, const vector<RowGroupRange> &ranges) {
	for (auto &other : ranges) {
		if (other.start >= range.end) {
			break;
		}
		if (range.Overlaps(other)) {
			return true;
		}
	}
	return false;
}

static bool IsPatchCovered(const RowGroupCollectionSnapshot &snapshot, const RowGroupRange &range) {
	if (snapshot.kind != RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT) {
		return false;
	}
	for (auto &patch : snapshot.layout->patches) {
		if (patch->range.start >= range.end) {
			break;
		}
		if (patch->range.Overlaps(range)) {
			return true;
		}
	}
	return false;
}

static void ValidateLimits(const ReclusterCandidateLimits &limits) {
	if (limits.max_physical_rows == 0 || limits.max_row_groups == 0 || limits.max_merge_runs < 2 ||
	    limits.max_merge_runs > 4 || !std::isfinite(limits.delete_cleanup_ratio) || limits.delete_cleanup_ratio <= 0 ||
	    limits.delete_cleanup_ratio > 1) {
		throw InternalException("Invalid recluster candidate limits");
	}
}

static void ValidateCheckpointSnapshot(const CheckpointLayoutSnapshot &checkpoint,
                                       sort_order_id_t current_sort_order_id) {
	optional<row_t> previous_end;
	unordered_set<sort_run_id_t> completed_current_runs;
	sort_run_id_t active_current_run = INVALID_SORT_RUN_ID;
	for (auto &identity : checkpoint.row_groups) {
		auto range = GetIdentityRange(identity);
		if (!identity.sort_metadata.IsValid()) {
			throw InternalException("Checkpoint row group identity has invalid sort metadata");
		}
		if (identity.format_version != 1 ||
		    ComputeRowGroupPhysicalIdentityChecksumV1(identity) != identity.immutable_data_checksum) {
			throw InternalException("Checkpoint row group identity checksum mismatch");
		}
		if (previous_end && range.start < *previous_end) {
			throw InternalException("Checkpoint row group identities are not ordered or overlap");
		}
		if (identity.sort_metadata.sort_order_id != current_sort_order_id) {
			if (active_current_run != INVALID_SORT_RUN_ID) {
				completed_current_runs.insert(active_current_run);
				active_current_run = INVALID_SORT_RUN_ID;
			}
		} else if (identity.sort_metadata.run_id != active_current_run) {
			if (active_current_run != INVALID_SORT_RUN_ID) {
				completed_current_runs.insert(active_current_run);
			}
			active_current_run = identity.sort_metadata.run_id;
			if (completed_current_runs.find(active_current_run) != completed_current_runs.end()) {
				throw InternalException("Checkpoint contains a non-contiguous current sort run");
			}
		}
		previous_end = range.end;
	}
}

static vector<CandidateRowGroupState> RevalidateCheckpointInputs(const vector<ColumnDefinition> &columns,
                                                                 const CheckpointLayoutSnapshot &checkpoint,
                                                                 const RowGroupCollectionSnapshot &current,
                                                                 const vector<RowGroupRange> &reserved_ranges) {
	vector<CandidateRowGroupState> result(checkpoint.row_groups.size());
	for (idx_t input_index = 0; input_index < checkpoint.row_groups.size(); input_index++) {
		auto &expected = checkpoint.row_groups[input_index];
		auto expected_range = GetIdentityRange(expected);
		if (!expected.sealed || IsPatchCovered(current, expected_range) ||
		    OverlapsAny(expected_range, reserved_ranges)) {
			continue;
		}

		LayoutRowGroupEntry current_entry;
		if (!current.Lookup(expected.start, current_entry) || current_entry.row_start != expected.start ||
		    current_entry.GetRowEnd() != expected_range.end) {
			continue;
		}
		auto current_identity =
		    ComputeRowGroupPhysicalIdentityV1(*current_entry.row_group, current_entry.row_start, columns);
		if (!current_identity || !(*current_identity == expected)) {
			continue;
		}

		auto live_rows = current_entry.row_group->GetCommittedRowCount();
		if (live_rows > expected.count) {
			throw InternalException("Recluster candidate has more committed rows than physical rows");
		}
		result[input_index].available = true;
		result[input_index].live_rows = live_rows;
	}
	return result;
}

static vector<CandidateUnit> BuildCandidateUnits(const CheckpointLayoutSnapshot &checkpoint,
                                                 const vector<CandidateRowGroupState> &input_states,
                                                 sort_order_id_t current_sort_order_id,
                                                 const vector<RowGroupRange> &reserved_ranges) {
	vector<CandidateUnit> units;
	idx_t input_index = 0;
	while (input_index < checkpoint.row_groups.size()) {
		auto &first = checkpoint.row_groups[input_index];
		auto current_run = first.sort_metadata.sort_order_id == current_sort_order_id;
		idx_t input_end = input_index + 1;
		if (current_run) {
			while (input_end < checkpoint.row_groups.size() &&
			       checkpoint.row_groups[input_end].sort_metadata.sort_order_id == current_sort_order_id &&
			       checkpoint.row_groups[input_end].sort_metadata.run_id == first.sort_metadata.run_id) {
				input_end++;
			}
		}

		CandidateUnit unit;
		unit.input_begin = input_index;
		unit.input_end = input_end;
		unit.range.start = first.start;
		unit.range.end = GetIdentityRange(checkpoint.row_groups[input_end - 1]).end;
		unit.current_run = current_run;
		unit.available = true;
		for (idx_t unit_index = input_index; unit_index < input_end; unit_index++) {
			auto &identity = checkpoint.row_groups[unit_index];
			unit.physical_rows = AddCount(unit.physical_rows, identity.count);
			unit.live_rows = AddCount(unit.live_rows, input_states[unit_index].live_rows);
			unit.row_group_count++;
			unit.available = unit.available && input_states[unit_index].available;
		}
		unit.deleted_rows = unit.physical_rows - unit.live_rows;
		unit.available = unit.available && !OverlapsAny(unit.range, reserved_ranges);
		units.push_back(unit);
		input_index = input_end;
	}
	return units;
}

static bool FitsLimits(idx_t physical_rows, idx_t row_group_count, const ReclusterCandidateLimits &limits) {
	return physical_rows <= limits.max_physical_rows && row_group_count <= limits.max_row_groups;
}

static bool MeetsDeleteThreshold(const CandidateUnit &unit, double threshold) {
	return unit.deleted_rows > 0 &&
	       static_cast<long double>(unit.deleted_rows) >= static_cast<long double>(unit.physical_rows) * threshold;
}

static ReclusterCandidate BuildCandidate(ReclusterCandidateType type, const CheckpointLayoutSnapshot &checkpoint,
                                         const vector<CandidateUnit> &units, idx_t unit_begin, idx_t unit_end,
                                         layout_version_t layout_version, sort_order_id_t sort_order_id) {
	D_ASSERT(unit_begin < unit_end);
	D_ASSERT(unit_end <= units.size());
	ReclusterCandidate result;
	result.type = type;
	result.range.start = units[unit_begin].range.start;
	result.range.end = units[unit_end - 1].range.end;
	result.checkpoint_number = checkpoint.checkpoint_number;
	result.storage_generation_id = checkpoint.storage_generation_id;
	result.layout_version = layout_version;
	result.sort_order_id = sort_order_id;
	for (idx_t unit_index = unit_begin; unit_index < unit_end; unit_index++) {
		auto &unit = units[unit_index];
		result.input_physical_rows = AddCount(result.input_physical_rows, unit.physical_rows);
		result.input_live_rows = AddCount(result.input_live_rows, unit.live_rows);
		result.input_deleted_rows = AddCount(result.input_deleted_rows, unit.deleted_rows);
		result.row_group_count = AddCount(result.row_group_count, unit.row_group_count);
		if (unit.current_run) {
			result.run_count++;
		}
		for (idx_t input_index = unit.input_begin; input_index < unit.input_end; input_index++) {
			result.expected_row_groups.push_back(checkpoint.row_groups[input_index]);
		}
	}
	return result;
}

static optional<ReclusterCandidate> SelectConversion(const CheckpointLayoutSnapshot &checkpoint,
                                                     const vector<CandidateUnit> &units,
                                                     const ReclusterCandidateLimits &limits,
                                                     layout_version_t layout_version, sort_order_id_t sort_order_id) {
	for (idx_t unit_begin = 0; unit_begin < units.size(); unit_begin++) {
		if (units[unit_begin].current_run || !units[unit_begin].available) {
			continue;
		}
		idx_t physical_rows = 0;
		idx_t row_group_count = 0;
		idx_t unit_end = unit_begin;
		while (unit_end < units.size() && !units[unit_end].current_run && units[unit_end].available) {
			auto next_physical_rows = AddCount(physical_rows, units[unit_end].physical_rows);
			auto next_row_group_count = AddCount(row_group_count, units[unit_end].row_group_count);
			if (!FitsLimits(next_physical_rows, next_row_group_count, limits)) {
				break;
			}
			physical_rows = next_physical_rows;
			row_group_count = next_row_group_count;
			unit_end++;
		}
		if (unit_end > unit_begin) {
			return BuildCandidate(ReclusterCandidateType::CONVERSION, checkpoint, units, unit_begin, unit_end,
			                      layout_version, sort_order_id);
		}
	}
	return nullopt;
}

static bool HasHigherDeleteRatio(const CandidateUnit &candidate, const CandidateUnit &best) {
	auto candidate_score =
	    Hugeint::Multiply(Hugeint::Convert(candidate.deleted_rows), Hugeint::Convert(best.physical_rows));
	auto best_score = Hugeint::Multiply(Hugeint::Convert(best.deleted_rows), Hugeint::Convert(candidate.physical_rows));
	return Hugeint::GreaterThan(candidate_score, best_score);
}

static optional<idx_t> SelectDeleteCleanup(const vector<CandidateUnit> &units, const ReclusterCandidateLimits &limits,
                                           bool &run_exceeds_limit) {
	optional<idx_t> best;
	for (idx_t unit_index = 0; unit_index < units.size(); unit_index++) {
		auto &unit = units[unit_index];
		if (!unit.current_run || !unit.available || !MeetsDeleteThreshold(unit, limits.delete_cleanup_ratio)) {
			continue;
		}
		if (!FitsLimits(unit.physical_rows, unit.row_group_count, limits)) {
			run_exceeds_limit = true;
			continue;
		}
		if (!best || HasHigherDeleteRatio(unit, units[*best])) {
			best = unit_index;
		}
	}
	return best;
}

static bool PreferMergeCandidate(const ReclusterCandidate &candidate, const ReclusterCandidate &best) {
	if (candidate.run_count != best.run_count) {
		return candidate.run_count > best.run_count;
	}
	if (candidate.input_physical_rows != best.input_physical_rows) {
		return candidate.input_physical_rows < best.input_physical_rows;
	}
	if (candidate.range.start != best.range.start) {
		return candidate.range.start < best.range.start;
	}
	return candidate.range.end < best.range.end;
}

static optional<ReclusterCandidate> SelectRunMerge(const CheckpointLayoutSnapshot &checkpoint,
                                                   const vector<CandidateUnit> &units,
                                                   const ReclusterCandidateLimits &limits,
                                                   layout_version_t layout_version, sort_order_id_t sort_order_id,
                                                   bool &run_exceeds_limit) {
	optional<ReclusterCandidate> best;
	for (idx_t unit_begin = 0; unit_begin < units.size(); unit_begin++) {
		if (!units[unit_begin].current_run || !units[unit_begin].available) {
			continue;
		}
		idx_t physical_rows = 0;
		idx_t row_group_count = 0;
		idx_t run_count = 0;
		for (idx_t unit_end = unit_begin; unit_end < units.size() && run_count < limits.max_merge_runs; unit_end++) {
			auto &unit = units[unit_end];
			if (!unit.current_run || !unit.available) {
				break;
			}
			physical_rows = AddCount(physical_rows, unit.physical_rows);
			row_group_count = AddCount(row_group_count, unit.row_group_count);
			run_count++;
			if (run_count < 2) {
				continue;
			}
			if (!FitsLimits(physical_rows, row_group_count, limits)) {
				run_exceeds_limit = true;
				break;
			}
			auto candidate = BuildCandidate(ReclusterCandidateType::RUN_MERGE, checkpoint, units, unit_begin,
			                                unit_end + 1, layout_version, sort_order_id);
			if (!best || PreferMergeCandidate(candidate, *best)) {
				best = std::move(candidate);
			}
		}
	}
	return best;
}

ReclusterCandidateSelection SelectReclusterCandidate(RowGroupCollection &collection,
                                                     const vector<ColumnDefinition> &columns,
                                                     TableReclusterState &state,
                                                     const ReclusterCandidateLimits &limits) {
	ValidateLimits(limits);
	if (!state.AcceptsNewTasks()) {
		return {};
	}
	auto checkpoint = state.GetLastCheckpoint();
	if (!checkpoint) {
		return {};
	}
	auto sort_order_id = state.GetCurrentSortOrderId();
	auto storage_generation_id = state.GetCurrentStorageGenerationId();
	if (sort_order_id == INVALID_SORT_ORDER_ID || checkpoint->storage_generation_id != storage_generation_id ||
	    collection.GetStorageGenerationId() != storage_generation_id) {
		return {};
	}

	ValidateCheckpointSnapshot(*checkpoint, sort_order_id);
	auto reserved_ranges = state.GetReservedRanges();
	auto current = collection.GetCurrentSnapshot();
	auto layout_version = current.kind == RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT
	                          ? current.layout->layout_version
	                          : INITIAL_LAYOUT_VERSION;
	auto input_states = RevalidateCheckpointInputs(columns, *checkpoint, current, reserved_ranges);
	auto units = BuildCandidateUnits(*checkpoint, input_states, sort_order_id, reserved_ranges);

	auto conversion = SelectConversion(*checkpoint, units, limits, layout_version, sort_order_id);
	if (conversion) {
		return {ReclusterCandidateSelectionStatus::SELECTED, std::move(conversion)};
	}

	bool run_exceeds_limit = false;
	auto cleanup_unit = SelectDeleteCleanup(units, limits, run_exceeds_limit);
	if (cleanup_unit) {
		auto candidate = BuildCandidate(ReclusterCandidateType::DELETE_CLEANUP, *checkpoint, units, *cleanup_unit,
		                                *cleanup_unit + 1, layout_version, sort_order_id);
		return {ReclusterCandidateSelectionStatus::SELECTED, std::move(candidate)};
	}

	auto merge = SelectRunMerge(*checkpoint, units, limits, layout_version, sort_order_id, run_exceeds_limit);
	if (merge) {
		return {ReclusterCandidateSelectionStatus::SELECTED, std::move(merge)};
	}
	return {run_exceeds_limit ? ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT
	                          : ReclusterCandidateSelectionStatus::NO_ELIGIBLE_RANGE,
	        nullopt};
}

static void ValidateCandidateEnvelope(const ReclusterCandidate &candidate) {
	if (candidate.range.start < 0 || candidate.range.start >= candidate.range.end ||
	    candidate.sort_order_id == INVALID_SORT_ORDER_ID || candidate.expected_row_groups.empty()) {
		throw InternalException("Invalid recluster candidate envelope");
	}
	if (candidate.expected_row_groups.front().start != candidate.range.start ||
	    GetIdentityRange(candidate.expected_row_groups.back()).end != candidate.range.end) {
		throw InternalException("Recluster candidate range does not match its row groups");
	}

	idx_t physical_rows = 0;
	idx_t row_group_count = 0;
	idx_t run_count = 0;
	RowGroupSortMetadata previous_metadata;
	for (auto &identity : candidate.expected_row_groups) {
		physical_rows = AddCount(physical_rows, identity.count);
		row_group_count++;
		if (identity.sort_metadata.sort_order_id == candidate.sort_order_id &&
		    identity.sort_metadata != previous_metadata) {
			run_count++;
		}
		previous_metadata = identity.sort_metadata;
	}
	if (candidate.input_physical_rows != physical_rows || candidate.row_group_count != row_group_count ||
	    candidate.run_count != run_count || candidate.input_live_rows > candidate.input_physical_rows ||
	    candidate.input_deleted_rows != candidate.input_physical_rows - candidate.input_live_rows) {
		throw InternalException("Recluster candidate counters do not match its row groups");
	}
}

static optional<idx_t> FindCandidateInCheckpoint(const CheckpointLayoutSnapshot &checkpoint,
                                                 const ReclusterCandidate &candidate) {
	for (idx_t checkpoint_index = 0; checkpoint_index < checkpoint.row_groups.size(); checkpoint_index++) {
		if (checkpoint.row_groups[checkpoint_index].start != candidate.range.start) {
			continue;
		}
		if (candidate.expected_row_groups.size() > checkpoint.row_groups.size() - checkpoint_index) {
			return nullopt;
		}
		for (idx_t candidate_index = 0; candidate_index < candidate.expected_row_groups.size(); candidate_index++) {
			if (!(checkpoint.row_groups[checkpoint_index + candidate_index] ==
			      candidate.expected_row_groups[candidate_index])) {
				return nullopt;
			}
		}
		return checkpoint_index;
	}
	return nullopt;
}

static bool SplitsCurrentRun(const CheckpointLayoutSnapshot &checkpoint, const ReclusterCandidate &candidate,
                             idx_t checkpoint_begin) {
	auto checkpoint_end = checkpoint_begin + candidate.expected_row_groups.size();
	auto &first = candidate.expected_row_groups.front().sort_metadata;
	if (first.sort_order_id == candidate.sort_order_id && checkpoint_begin > 0 &&
	    checkpoint.row_groups[checkpoint_begin - 1].sort_metadata == first) {
		return true;
	}
	auto &last = candidate.expected_row_groups.back().sort_metadata;
	return last.sort_order_id == candidate.sort_order_id && checkpoint_end < checkpoint.row_groups.size() &&
	       checkpoint.row_groups[checkpoint_end].sort_metadata == last;
}

optional<ReclusterCandidate> RevalidateReclusterCandidate(RowGroupCollection &collection,
                                                          const vector<ColumnDefinition> &columns,
                                                          TableReclusterState &state,
                                                          const ReclusterCandidate &candidate) {
	ValidateCandidateEnvelope(candidate);
	if (!state.AcceptsNewTasks() || state.GetCurrentSortOrderId() != candidate.sort_order_id ||
	    state.GetCurrentStorageGenerationId() != candidate.storage_generation_id ||
	    collection.GetStorageGenerationId() != candidate.storage_generation_id) {
		return nullopt;
	}
	auto checkpoint = state.GetLastCheckpoint();
	if (!checkpoint || checkpoint->checkpoint_number != candidate.checkpoint_number ||
	    checkpoint->storage_generation_id != candidate.storage_generation_id) {
		return nullopt;
	}
	ValidateCheckpointSnapshot(*checkpoint, candidate.sort_order_id);
	auto checkpoint_begin = FindCandidateInCheckpoint(*checkpoint, candidate);
	if (!checkpoint_begin || SplitsCurrentRun(*checkpoint, candidate, *checkpoint_begin)) {
		return nullopt;
	}

	auto current = collection.GetCurrentSnapshot();
	auto layout_version = current.kind == RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT
	                          ? current.layout->layout_version
	                          : INITIAL_LAYOUT_VERSION;
	if (layout_version != candidate.layout_version || IsPatchCovered(current, candidate.range) ||
	    OverlapsAny(candidate.range, state.GetReservedRanges())) {
		return nullopt;
	}

	ReclusterCandidate result = candidate;
	result.input_live_rows = 0;
	for (auto &expected : candidate.expected_row_groups) {
		if (!expected.sealed) {
			return nullopt;
		}
		auto expected_range = GetIdentityRange(expected);
		LayoutRowGroupEntry current_entry;
		if (!current.Lookup(expected.start, current_entry) || current_entry.row_start != expected.start ||
		    current_entry.GetRowEnd() != expected_range.end) {
			return nullopt;
		}
		auto current_identity =
		    ComputeRowGroupPhysicalIdentityV1(*current_entry.row_group, current_entry.row_start, columns);
		if (!current_identity || !(*current_identity == expected)) {
			return nullopt;
		}
		auto live_rows = current_entry.row_group->GetCommittedRowCount();
		if (live_rows > expected.count) {
			throw InternalException("Recluster candidate has more committed rows than physical rows");
		}
		result.input_live_rows = AddCount(result.input_live_rows, live_rows);
	}
	result.input_deleted_rows = result.input_physical_rows - result.input_live_rows;
	return result;
}

} // namespace duckdb
