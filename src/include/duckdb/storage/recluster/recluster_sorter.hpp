//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_sorter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

class DataChunk;
class GlobalSinkState;
class GlobalSourceState;
class LocalSourceState;
class RangeTask;
class ReclusterTaskContext;
class Sort;
class ThreadContext;

class ReclusterSorter {
	friend class ReclusterSortSinkTask;

public:
	explicit ReclusterSorter(RangeTask &task);
	~ReclusterSorter();

	void Prepare();
	void InitializeChunk(DataChunk &chunk) const;
	bool Scan(DataChunk &chunk);

	const vector<LogicalType> &GetOutputTypes() const {
		return output_types;
	}
	idx_t GetInputRowCount() const {
		return input_row_count.load();
	}
	idx_t GetSortedRowCount() const {
		return sorted_row_count;
	}
	bool IsFinished() const {
		return finished;
	}

private:
	void CheckTask() const;
	void Finish();
	void ResetSortResources();
	void SinkRange(RowGroupRange range);

private:
	RangeTask &task;
	ReclusterTaskContext &task_context;
	vector<LogicalType> output_types;
	unique_ptr<ThreadContext> thread_context;
	unique_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> global_sink;
	unique_ptr<GlobalSourceState> global_source;
	unique_ptr<LocalSourceState> local_source;
	atomic<idx_t> input_row_count {0};
	idx_t sorted_row_count = 0;
	bool prepared = false;
	bool finished = false;
};

} // namespace duckdb
