//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_run_merger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/create_sort_key.hpp"

namespace duckdb {

class RangeTask;
class ReclusterTaskContext;

class ReclusterRunMerger {
public:
	explicit ReclusterRunMerger(RangeTask &task);
	~ReclusterRunMerger();

	void Prepare();
	bool Scan(DataChunk &chunk);

	idx_t GetOutputRowCount() const {
		return output_row_count;
	}

private:
	struct RunState;

	void CheckTask() const;
	void BuildRunStates();
	bool FillRun(RunState &run);
	bool RebuildHeap();
	bool HeapAfter(idx_t left_run, idx_t right_run) const;
	string_t GetCurrentKey(idx_t run_index) const;

private:
	RangeTask &task;
	ReclusterTaskContext &task_context;
	vector<LogicalType> output_types;
	vector<column_t> sort_columns;
	vector<OrderModifiers> sort_modifiers;
	vector<unique_ptr<RunState>> runs;
	vector<idx_t> heap;
	DataChunk source_rows;
	idx_t source_capacity = 0;
	idx_t output_row_count = 0;
	string last_output_key;
	bool has_last_output_key = false;
	bool prepared = false;
	bool needs_rebuild = true;
};

} // namespace duckdb
