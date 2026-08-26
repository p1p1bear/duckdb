#include "duckdb/storage/recluster/recluster_sorter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_range_scanner.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_sort_bind.hpp"

namespace duckdb {

ReclusterSorter::ReclusterSorter(RangeTask &task_p) : task(task_p), task_context(task.GetTaskContext()) {
	if (task.GetState() != RangeTaskState::PREPARING || !task_context.HasActiveSnapshot()) {
		throw InternalException("Recluster sorter requires a preparing task with an active snapshot");
	}
}

ReclusterSorter::~ReclusterSorter() {
	ResetSortResources();
	task_context.CloseSnapshot();
}

void ReclusterSorter::CheckTask() const {
	if (task.IsCancelRequested() || task.IsPublishForbidden()) {
		throw InterruptException("Recluster task was cancelled");
	}
	if (task.GetState() != RangeTaskState::PREPARING) {
		throw InternalException("Recluster sorter observed an invalid task state");
	}
	task_context.InterruptCheck();
}

void ReclusterSorter::ResetSortResources() {
	local_source.reset();
	global_source.reset();
	global_sink.reset();
	sort.reset();
	thread_context.reset();
}

void ReclusterSorter::Finish() {
	if (sorted_row_count != input_row_count.load() ||
	    task_context.GetRowIdRemap().GetMappedCount() != input_row_count.load()) {
		throw InternalException("Recluster sort output does not match its input row count");
	}
	finished = true;
	ResetSortResources();
	task_context.CloseSnapshot();
}

void ReclusterSorter::SinkRange(RowGroupRange range) {
	CheckTask();
	ReclusterRangeScanner scanner(task_context, range);
	auto &context = task_context.GetSnapshotContext();
	ThreadContext local_thread_context(context);
	ExecutionContext execution(context, local_thread_context, nullptr);
	auto local_sink = sort->GetLocalSinkState(execution);
	DataChunk input;
	scanner.InitializeChunk(input);
	while (scanner.Scan(input)) {
		CheckTask();
		while (true) {
			auto signal = make_shared_ptr<InterruptDoneSignalState>();
			InterruptState interrupt {weak_ptr<InterruptDoneSignalState>(signal)};
			OperatorSinkInput sink_input {*global_sink, *local_sink, interrupt};
			auto result = sort->Sink(execution, input, sink_input);
			if (result != SinkResultType::BLOCKED) {
				if (result != SinkResultType::NEED_MORE_INPUT) {
					throw InternalException("Recluster sorter stopped accepting input");
				}
				break;
			}
			signal->Await();
			CheckTask();
		}
	}

	while (true) {
		auto signal = make_shared_ptr<InterruptDoneSignalState>();
		InterruptState interrupt {weak_ptr<InterruptDoneSignalState>(signal)};
		OperatorSinkCombineInput combine_input {*global_sink, *local_sink, interrupt};
		auto result = sort->Combine(execution, combine_input);
		if (result != SinkCombineResultType::BLOCKED) {
			break;
		}
		signal->Await();
		CheckTask();
	}
	input_row_count.fetch_add(scanner.GetScannedRowCount());
}

class ReclusterSortSinkTask : public BaseExecutorTask {
public:
	ReclusterSortSinkTask(TaskExecutor &executor, ReclusterSorter &sorter_p, RowGroupRange range_p)
	    : BaseExecutorTask(executor), sorter(sorter_p), range(range_p) {
	}

	void ExecuteTask() override {
		sorter.SinkRange(range);
	}

	string TaskType() const override {
		return "ReclusterSortSinkTask";
	}

private:
	ReclusterSorter &sorter;
	RowGroupRange range;
};

void ReclusterSorter::Prepare() {
	if (prepared || finished) {
		throw InternalException("Recluster sorter was prepared more than once");
	}
	try {
		CheckTask();
		output_types = ReclusterRangeScanner::GetOutputTypes(task_context);
		auto &context = task_context.GetSnapshotContext();
		auto orders = BuildPersistentSortOrders(task_context.GetSortDefinition(), task_context.GetPhysicalSortIndexes(),
		                                        output_types, output_types.size() - 1);
		sort = make_uniq<Sort>(context, orders, output_types, vector<idx_t>());
		global_sink = sort->GetGlobalSinkState(context);
		TaskExecutor executor(context);
		for (auto &row_group : task_context.GetCandidate().expected_row_groups) {
			auto range = RowGroupRange {row_group.start, row_group.start + NumericCast<row_t>(row_group.count)};
			executor.ScheduleTask(make_uniq<ReclusterSortSinkTask>(executor, *this, range));
		}
		executor.WorkOnTasks();
		thread_context = make_uniq<ThreadContext>(context);
		ExecutionContext execution(context, *thread_context, nullptr);

		SinkFinalizeType finalize_result;
		while (true) {
			auto signal = make_shared_ptr<InterruptDoneSignalState>();
			InterruptState interrupt {weak_ptr<InterruptDoneSignalState>(signal)};
			OperatorSinkFinalizeInput finalize_input {*global_sink, interrupt};
			finalize_result = sort->Finalize(context, finalize_input);
			if (finalize_result != SinkFinalizeType::BLOCKED) {
				break;
			}
			signal->Await();
			CheckTask();
		}
		prepared = true;
		if (finalize_result == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			if (input_row_count.load() != 0) {
				throw InternalException("Recluster sorter lost non-empty input");
			}
			Finish();
			return;
		}
		global_source = sort->GetGlobalSourceState(context, *global_sink);
		local_source = sort->GetLocalSourceState(execution, *global_source);
	} catch (...) {
		ResetSortResources();
		task_context.CloseSnapshot();
		throw;
	}
}

void ReclusterSorter::InitializeChunk(DataChunk &chunk) const {
	if (!prepared) {
		throw InternalException("Cannot initialize recluster sort output before Prepare");
	}
	chunk.Initialize(Allocator::DefaultAllocator(), output_types);
}

bool ReclusterSorter::Scan(DataChunk &chunk) {
	if (!prepared) {
		throw InternalException("Cannot scan recluster sort output before Prepare");
	}
	if (finished) {
		chunk.Reset();
		return false;
	}
	try {
		CheckTask();
		if (chunk.ColumnCount() != output_types.size()) {
			throw InternalException("Recluster sort output chunk has an invalid column count");
		}
		ExecutionContext execution(task_context.GetSnapshotContext(), *thread_context, nullptr);
		SourceResultType result;
		while (true) {
			auto signal = make_shared_ptr<InterruptDoneSignalState>();
			InterruptState interrupt {weak_ptr<InterruptDoneSignalState>(signal)};
			OperatorSourceInput source_input {*global_source, *local_source, interrupt};
			chunk.Reset();
			result = sort->GetData(execution, chunk, source_input);
			if (result != SourceResultType::BLOCKED) {
				break;
			}
			signal->Await();
			CheckTask();
		}
		if (result == SourceResultType::FINISHED) {
			if (chunk.size() != 0) {
				throw InternalException("Finished recluster sorter returned output");
			}
			Finish();
			return false;
		}
		if (result != SourceResultType::HAVE_MORE_OUTPUT || chunk.size() == 0) {
			throw InternalException("Recluster sorter returned an invalid source result");
		}
		CheckTask();

		auto &range = task.GetRange();
		auto range_size = NumericCast<idx_t>(range.end - range.start);
		if (sorted_row_count > range_size || chunk.size() > range_size - sorted_row_count) {
			throw InternalException("Recluster sort output exceeds the reserved row ID range");
		}
		auto &old_rowids = chunk.data.back();
		UnifiedVectorFormat rowid_data;
		old_rowids.ToUnifiedFormat(rowid_data);
		auto rowids = UnifiedVectorFormat::GetData<row_t>(rowid_data);
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			auto source_index = rowid_data.sel->get_index(row_index);
			if (!rowid_data.validity.RowIsValid(source_index)) {
				throw InternalException("Recluster sort produced a NULL old row ID");
			}
			auto new_rowid = range.start + NumericCast<row_t>(sorted_row_count + row_index);
			task_context.GetRowIdRemap().SetNewRowId(rowids[source_index], new_rowid);
		}
		sorted_row_count += chunk.size();
		return true;
	} catch (...) {
		ResetSortResources();
		task_context.CloseSnapshot();
		throw;
	}
}

} // namespace duckdb
