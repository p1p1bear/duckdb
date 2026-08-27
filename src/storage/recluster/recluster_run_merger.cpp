#include "duckdb/storage/recluster/recluster_run_merger.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_range_scanner.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"

#include <algorithm>

namespace duckdb {

struct ReclusterRunMerger::RunState {
	RunState(ReclusterTaskContext &task_context, RowGroupRange range_p, idx_t slot_offset_p)
	    : range(range_p), slot_offset(slot_offset_p), scanner(make_uniq<ReclusterRangeScanner>(task_context, range)) {
		scanner->InitializeChunk(input);
	}

	RowGroupRange range;
	idx_t slot_offset;
	unique_ptr<ReclusterRangeScanner> scanner;
	DataChunk input;
	unique_ptr<Vector> sort_keys;
	const string_t *key_data = nullptr;
	idx_t position = 0;
	idx_t count = 0;
	string last_input_key;
	bool has_last_input_key = false;
	bool finished = false;
};

ReclusterRunMerger::ReclusterRunMerger(RangeTask &task_p) : task(task_p), task_context(task.GetTaskContext()) {
	if (task.GetState() != RangeTaskState::PREPARING || !task_context.HasActiveSnapshot()) {
		throw InternalException("Recluster run merger requires a preparing task with an active snapshot");
	}
}

ReclusterRunMerger::~ReclusterRunMerger() {
}

void ReclusterRunMerger::CheckTask() const {
	if (task.IsCancelRequested() || task.IsPublishForbidden()) {
		throw InterruptException("Recluster task was cancelled during run merge");
	}
	if (task.GetState() != RangeTaskState::PREPARING) {
		throw InternalException("Recluster run merger observed an invalid task state");
	}
	task_context.InterruptCheck();
}

void ReclusterRunMerger::BuildRunStates() {
	auto &candidate = task_context.GetCandidate();
	if (candidate.type == ReclusterCandidateType::CONVERSION || candidate.run_count == 0) {
		throw InternalException("Recluster run merger requires at least one current sorted run");
	}

	optional<RowGroupSortMetadata> active_metadata;
	RowGroupRange active_range {0, 0};
	for (auto &identity : candidate.expected_row_groups) {
		if (identity.sort_metadata.sort_order_id != candidate.sort_order_id ||
		    identity.sort_metadata.run_id == INVALID_SORT_RUN_ID) {
			throw InternalException("Recluster run merger input is not part of the current sort order");
		}
		auto identity_end = identity.start + NumericCast<row_t>(identity.count);
		if (!active_metadata || identity.sort_metadata != *active_metadata) {
			if (active_metadata) {
				auto slot_offset = runs.size() * STANDARD_VECTOR_SIZE;
				runs.push_back(make_uniq<RunState>(task_context, active_range, slot_offset));
			}
			active_metadata = identity.sort_metadata;
			active_range = {identity.start, identity_end};
		} else {
			if (identity.start != active_range.end) {
				throw InternalException("Recluster sorted run contains a physical row ID gap");
			}
			active_range.end = identity_end;
		}
	}
	if (active_metadata) {
		auto slot_offset = runs.size() * STANDARD_VECTOR_SIZE;
		runs.push_back(make_uniq<RunState>(task_context, active_range, slot_offset));
	}
	if (runs.size() != candidate.run_count) {
		throw InternalException("Recluster run merger input run count changed");
	}
}

void ReclusterRunMerger::Prepare() {
	if (prepared) {
		throw InternalException("Recluster run merger was prepared more than once");
	}
	CheckTask();
	output_types = ReclusterRangeScanner::GetOutputTypes(task_context);
	for (auto physical_index : task_context.GetPhysicalSortIndexes()) {
		if (physical_index >= output_types.size() - 1) {
			throw InternalException("Recluster run merger sort column is outside its scan schema");
		}
		sort_columns.push_back(NumericCast<column_t>(physical_index));
	}
	for (auto &column : task_context.GetSortDefinition().columns) {
		sort_modifiers.emplace_back(column.order_type, column.null_order);
	}
	if (sort_columns.empty() || sort_columns.size() != sort_modifiers.size()) {
		throw InternalException("Recluster run merger has an invalid sort definition");
	}

	BuildRunStates();
	if (runs.size() > NumericLimits<idx_t>::Maximum() / STANDARD_VECTOR_SIZE) {
		throw InternalException("Recluster run merger source capacity overflow");
	}
	source_capacity = runs.size() * STANDARD_VECTOR_SIZE;
	source_rows.Initialize(task_context.GetSnapshotContext(), output_types, source_capacity);
	source_rows.SetChildCardinality(source_capacity);
	prepared = true;
}

bool ReclusterRunMerger::FillRun(RunState &run) {
	CheckTask();
	if (run.finished) {
		return false;
	}
	if (!run.scanner->Scan(run.input)) {
		run.finished = true;
		run.position = 0;
		run.count = 0;
		run.key_data = nullptr;
		run.sort_keys.reset();
		return false;
	}
	if (run.input.size() == 0 || run.input.size() > STANDARD_VECTOR_SIZE) {
		throw InternalException("Recluster run merger scanner returned an invalid chunk size");
	}

	for (idx_t column_index = 0; column_index < output_types.size(); column_index++) {
		VectorOperations::Copy(run.input.data[column_index], source_rows.data[column_index], run.input.size(), 0,
		                       run.slot_offset);
	}

	DataChunk key_input;
	vector<LogicalType> key_types;
	key_types.reserve(sort_columns.size());
	for (auto column_index : sort_columns) {
		key_types.push_back(output_types[column_index]);
	}
	key_input.InitializeEmpty(key_types);
	key_input.ReferenceColumns(run.input, sort_columns);
	run.sort_keys = make_uniq<Vector>(LogicalType::BLOB, run.input.size());
	CreateSortKeyHelpers::CreateSortKey(key_input, sort_modifiers, *run.sort_keys);
	run.key_data = FlatVector::GetData<string_t>(*run.sort_keys);
	run.position = 0;
	run.count = run.input.size();

	if (run.has_last_input_key) {
		auto previous = string_t(run.last_input_key.data(), NumericCast<uint32_t>(run.last_input_key.size()));
		if (run.key_data[0] < previous) {
			throw InternalException("Recluster sorted run metadata does not match its physical row order");
		}
	}
	for (idx_t row_index = 1; row_index < run.count; row_index++) {
		if (run.key_data[row_index] < run.key_data[row_index - 1]) {
			throw InternalException("Recluster sorted run metadata does not match its physical row order");
		}
	}
	auto &last_key = run.key_data[run.count - 1];
	run.last_input_key.assign(last_key.GetData(), last_key.GetSize());
	run.has_last_input_key = true;
	return true;
}

string_t ReclusterRunMerger::GetCurrentKey(idx_t run_index) const {
	auto &run = *runs[run_index];
	D_ASSERT(run.position < run.count);
	D_ASSERT(run.key_data);
	return run.key_data[run.position];
}

bool ReclusterRunMerger::HeapAfter(idx_t left_run, idx_t right_run) const {
	auto left_key = GetCurrentKey(left_run);
	auto right_key = GetCurrentKey(right_run);
	if (left_key == right_key) {
		return left_run > right_run;
	}
	return left_key > right_key;
}

bool ReclusterRunMerger::RebuildHeap() {
	heap.clear();
	for (idx_t run_index = 0; run_index < runs.size(); run_index++) {
		auto &run = *runs[run_index];
		if (run.position >= run.count && !FillRun(run)) {
			continue;
		}
		heap.push_back(run_index);
	}
	auto comparator = [this](idx_t left, idx_t right) {
		return HeapAfter(left, right);
	};
	std::make_heap(heap.begin(), heap.end(), comparator);
	needs_rebuild = false;
	return !heap.empty();
}

bool ReclusterRunMerger::Scan(DataChunk &chunk) {
	if (!prepared) {
		throw InternalException("Cannot scan a recluster run merge before Prepare");
	}
	CheckTask();
	chunk.Reset();
	if (needs_rebuild && !RebuildHeap()) {
		return false;
	}
	if (heap.empty()) {
		throw InternalException("Recluster run merger lost its active input heap");
	}

	SelectionVector selection(STANDARD_VECTOR_SIZE);
	auto comparator = [this](idx_t left, idx_t right) {
		return HeapAfter(left, right);
	};
	idx_t output_count = 0;
	string_t previous_key;
	bool has_previous_key = false;
	while (output_count < STANDARD_VECTOR_SIZE && !heap.empty()) {
		std::pop_heap(heap.begin(), heap.end(), comparator);
		auto run_index = heap.back();
		heap.pop_back();
		auto &run = *runs[run_index];
		auto current_key = GetCurrentKey(run_index);
		if (!has_previous_key && has_last_output_key) {
			auto last_key = string_t(last_output_key.data(), NumericCast<uint32_t>(last_output_key.size()));
			if (current_key < last_key) {
				throw InternalException("Recluster run merge produced an out-of-order key");
			}
		} else if (has_previous_key && current_key < previous_key) {
			throw InternalException("Recluster run merge produced an out-of-order key");
		}

		selection.set_index(output_count++, NumericCast<sel_t>(run.slot_offset + run.position));
		previous_key = current_key;
		has_previous_key = true;
		run.position++;
		if (run.position < run.count) {
			heap.push_back(run_index);
			std::push_heap(heap.begin(), heap.end(), comparator);
		} else {
			needs_rebuild = true;
			break;
		}
	}
	if (output_count == 0 || !has_previous_key) {
		throw InternalException("Recluster run merger produced an empty output batch");
	}
	chunk.Append(source_rows, selection, output_count);
	last_output_key.assign(previous_key.GetData(), previous_key.GetSize());
	has_last_output_key = true;
	output_row_count += output_count;
	return true;
}

} // namespace duckdb
