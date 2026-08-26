#include "duckdb/execution/operator/persistent/adaptive_sorted_write.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/recluster/table_sort_bind.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"

namespace duckdb {

InsertOrderToken InsertOrderToken::ArrivalOrder() {
	return {};
}

InsertOrderToken InsertOrderToken::BatchOrder(idx_t batch_index, idx_t chunk_index) {
	return {optional_idx(batch_index), chunk_index};
}

bool InsertOrderToken::operator<(const InsertOrderToken &other) const {
	if (batch_index.IsValid() != other.batch_index.IsValid()) {
		return !batch_index.IsValid();
	}
	if (batch_index.IsValid() && batch_index.GetIndex() != other.batch_index.GetIndex()) {
		return batch_index.GetIndex() < other.batch_index.GetIndex();
	}
	return chunk_index < other.chunk_index;
}

AdaptiveSortedWrite::AdaptiveSortedWrite(ClientContext &context, DuckTableEntry &table_p,
                                         vector<LogicalType> input_types_p,
                                         const vector<unique_ptr<BoundConstraint>> &bound_constraints_p)
    : table(table_p), input_types(std::move(input_types_p)), bound_constraints(bound_constraints_p),
      phase(AdaptiveInsertPhase::BUFFERING), total_count(0), row_group_size(table.GetStorage().GetRowGroupSize()),
      staged_count(0), next_arrival_token(0), token_mode_initialized(false), uses_batch_tokens(false),
      run_id(INVALID_SORT_RUN_ID) {
	(void)context;
	auto metadata = table.GetSortMetadata();
	if (!metadata || !metadata->IsEnabled()) {
		throw InternalException("Adaptive sorted write requires an enabled SORTED BY definition");
	}
	auto current = metadata->GetCurrent();
	if (!current) {
		throw InternalException("Adaptive sorted write cannot find the current SORTED BY definition");
	}
	sort_definition = *current;
	if (row_group_size == 0) {
		throw InternalException("Adaptive sorted write requires a non-zero row group size");
	}
}

AdaptiveSortedWrite::~AdaptiveSortedWrite() {
	RollbackDrainWriters();
}

InsertOrderToken AdaptiveSortedWrite::ResolveOrderToken(const InsertOrderToken &order_token) {
	auto has_batch_token = order_token.batch_index.IsValid();
	if (!token_mode_initialized) {
		token_mode_initialized = true;
		uses_batch_tokens = has_batch_token;
	} else if (uses_batch_tokens != has_batch_token) {
		throw InternalException("Adaptive sorted write cannot mix arrival and batch order tokens");
	}
	if (has_batch_token) {
		return order_token;
	}
	return {optional_idx(), next_arrival_token++};
}

void AdaptiveSortedWrite::EnsureWriteGate(ClientContext &context) {
	{
		lock_guard<mutex> guard(lock);
		if (write_gate_held) {
			return;
		}
	}
	table.GetStorage().HoldReclusterWriteGate(context, "insert into");
	lock_guard<mutex> guard(lock);
	write_gate_held = true;
}

void AdaptiveSortedWrite::AppendOwned(ClientContext &context, DataChunk &chunk, idx_t offset, idx_t count,
                                      const InsertOrderToken &order_token) {
	D_ASSERT(count > 0);
	D_ASSERT(offset + count <= chunk.size());
	auto owned = make_uniq<DataChunk>();
	owned->Initialize(context, input_types, count);
	if (offset == 0 && count == chunk.size()) {
		chunk.Copy(*owned);
	} else {
		DataChunk slice;
		slice.InitializeEmpty(input_types);
		slice.Slice(chunk, offset, offset + count);
		slice.Copy(*owned);
	}
	if (!staging.emplace(order_token, std::move(owned)).second) {
		throw InternalException("Adaptive sorted write received a duplicate insert order token");
	}
	staged_count += count;
	D_ASSERT(staged_count <= row_group_size);
}

void AdaptiveSortedWrite::SetFailure(std::exception_ptr exception) {
	{
		lock_guard<mutex> guard(lock);
		if (phase != AdaptiveInsertPhase::FAILED) {
			failure = std::move(exception);
			phase = AdaptiveInsertPhase::FAILED;
		}
	}
	phase_changed.notify_all();
}

void AdaptiveSortedWrite::ThrowFailure(unique_lock<mutex> &guard) const {
	D_ASSERT(phase == AdaptiveInsertPhase::FAILED);
	auto exception = failure;
	guard.unlock();
	if (exception) {
		std::rethrow_exception(exception);
	}
	throw InternalException("Adaptive sorted write failed without an exception");
}

void AdaptiveSortedWrite::InitializeSort(ClientContext &context) {
	auto physical_indexes = BindPersistentSortIndexes(table.GetStorage().Columns(), sort_definition);
	auto orders = BuildPersistentSortOrders(sort_definition, physical_indexes, input_types, input_types.size());
	auto initialized_sort = make_uniq<Sort>(context, orders, input_types, vector<idx_t>());
	auto initialized_sink = initialized_sort->GetGlobalSinkState(context);
	{
		lock_guard<mutex> guard(lock);
		if (phase != AdaptiveInsertPhase::INITIALIZING_SORT) {
			throw InternalException("Adaptive sorted write changed phase while initializing its sorter");
		}
		sort = std::move(initialized_sort);
		sort_sink = std::move(initialized_sink);
		phase = AdaptiveInsertPhase::SORTING;
	}
	phase_changed.notify_all();
}

void AdaptiveSortedWrite::SinkToSort(ExecutionContext &context, DataChunk &chunk,
                                     AdaptiveSortedWriteLocalState &local_state, InterruptState &interrupt_state) {
	if (!local_state.sort_sink) {
		local_state.sort_sink = sort->GetLocalSinkState(context);
	}
	OperatorSinkInput sort_input {*sort_sink, *local_state.sort_sink, interrupt_state};
	auto result = sort->Sink(context, chunk, sort_input);
	if (result != SinkResultType::NEED_MORE_INPUT) {
		throw InternalException("Adaptive sorted write sorter unexpectedly blocked");
	}
}

SinkResultType AdaptiveSortedWrite::SinkInternal(ExecutionContext &context, DataChunk &chunk,
                                                 AdaptiveSortedWriteLocalState &local_state,
                                                 const InsertOrderToken &order_token, InterruptState &interrupt_state) {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	EnsureWriteGate(context.client);

	OrderedInsertStaging initial_staging;
	idx_t suffix_offset = 0;
	bool initialize_sort = false;
	{
		unique_lock<mutex> guard(lock);
		if (phase == AdaptiveInsertPhase::FAILED) {
			ThrowFailure(guard);
		}
		if (total_count > NumericLimits<idx_t>::Maximum() - chunk.size()) {
			throw OutOfRangeException("Adaptive sorted write row count overflow");
		}
		auto previous_total = total_count;
		total_count += chunk.size();

		if (phase == AdaptiveInsertPhase::BUFFERING) {
			auto resolved_token = ResolveOrderToken(order_token);
			if (total_count < row_group_size) {
				AppendOwned(context.client, chunk, 0, chunk.size(), resolved_token);
				return SinkResultType::NEED_MORE_INPUT;
			}

			auto prefix_count = row_group_size - previous_total;
			AppendOwned(context.client, chunk, 0, prefix_count, resolved_token);
			run_id = table.GetStorage().GetDataTableInfo()->GetSortStorage().AllocateRunId();
			phase = AdaptiveInsertPhase::INITIALIZING_SORT;
			initial_staging = std::move(staging);
			suffix_offset = prefix_count;
			initialize_sort = true;
		} else {
			while (phase == AdaptiveInsertPhase::INITIALIZING_SORT) {
				phase_changed.wait(guard);
			}
			if (phase == AdaptiveInsertPhase::FAILED) {
				ThrowFailure(guard);
			}
			if (phase != AdaptiveInsertPhase::SORTING) {
				throw InternalException("Adaptive sorted write received input after sink completion");
			}
		}
	}

	if (initialize_sort) {
		InitializeSort(context.client);
		for (auto &entry : initial_staging) {
			SinkToSort(context, *entry.second, local_state, interrupt_state);
		}
		if (suffix_offset < chunk.size()) {
			DataChunk suffix;
			suffix.InitializeEmpty(input_types);
			suffix.Slice(chunk, suffix_offset, chunk.size());
			SinkToSort(context, suffix, local_state, interrupt_state);
		}
	} else {
		SinkToSort(context, chunk, local_state, interrupt_state);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType AdaptiveSortedWrite::Sink(ExecutionContext &context, DataChunk &chunk,
                                         AdaptiveSortedWriteLocalState &local_state,
                                         const InsertOrderToken &order_token, InterruptState &interrupt_state) {
	try {
		return SinkInternal(context, chunk, local_state, order_token, interrupt_state);
	} catch (...) {
		SetFailure(std::current_exception());
		throw;
	}
}

SinkCombineResultType AdaptiveSortedWrite::CombineInternal(ExecutionContext &context,
                                                           AdaptiveSortedWriteLocalState &local_state,
                                                           InterruptState &interrupt_state) {
	{
		unique_lock<mutex> guard(lock);
		while (phase == AdaptiveInsertPhase::INITIALIZING_SORT) {
			phase_changed.wait(guard);
		}
		if (phase == AdaptiveInsertPhase::FAILED) {
			ThrowFailure(guard);
		}
		if (phase == AdaptiveInsertPhase::BUFFERING || !local_state.sort_sink) {
			return SinkCombineResultType::FINISHED;
		}
		if (phase != AdaptiveInsertPhase::SORTING) {
			throw InternalException("Adaptive sorted write combined after sink completion");
		}
	}
	OperatorSinkCombineInput sort_input {*sort_sink, *local_state.sort_sink, interrupt_state};
	auto result = sort->Combine(context, sort_input);
	local_state.sort_sink.reset();
	return result;
}

SinkCombineResultType AdaptiveSortedWrite::Combine(ExecutionContext &context,
                                                   AdaptiveSortedWriteLocalState &local_state,
                                                   InterruptState &interrupt_state) {
	try {
		return CombineInternal(context, local_state, interrupt_state);
	} catch (...) {
		SetFailure(std::current_exception());
		throw;
	}
}

void AdaptiveSortedWrite::DrainUnsorted(ClientContext &context, OrderedInsertStaging owned_staging) {
	if (total_count == 0) {
		return;
	}
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, table, context, bound_constraints, AppendOrganization::Unsorted());
	for (auto &entry : owned_staging) {
		storage.LocalAppend(append_state, table, context, *entry.second, true);
	}
	storage.FinalizeLocalAppend(append_state);
}

PhysicalIndex AdaptiveSortedWrite::CreateDrainCollection(ClientContext &context,
                                                         unique_ptr<OptimisticWriteCollection> collection) {
	lock_guard<mutex> guard(lock);
	if (phase != AdaptiveInsertPhase::DRAINING) {
		throw InternalException("Adaptive sorted write created a drain collection outside the drain phase");
	}
	return table.GetStorage().CreateOptimisticCollection(context, std::move(collection));
}

void AdaptiveSortedWrite::RegisterDrainPartition(idx_t partition_index, PhysicalIndex collection_index,
                                                 idx_t row_count) {
	lock_guard<mutex> guard(lock);
	if (phase != AdaptiveInsertPhase::DRAINING || row_count == 0) {
		throw InternalException("Adaptive sorted write registered an invalid drain partition");
	}
	drain_partitions.push_back({partition_index, collection_index, row_count});
	drained_row_count += row_count;
}

void AdaptiveSortedWrite::RegisterDrainWriter(unique_ptr<OptimisticDataWriter> writer) {
	if (!writer) {
		return;
	}
	lock_guard<mutex> guard(lock);
	if (phase != AdaptiveInsertPhase::DRAINING) {
		throw InternalException("Adaptive sorted write registered a writer outside the drain phase");
	}
	drain_writers.push_back(std::move(writer));
}

void AdaptiveSortedWrite::PrepareParallelDrain(ClientContext &context) {
	auto max_threads = sort_source->MaxThreads();
	if (max_threads == 0) {
		throw InternalException("Adaptive sorted write sorter has no output partitions");
	}
	auto scheduler_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	single_collection_drain = row_group_size != DEFAULT_ROW_GROUP_SIZE || scheduler_threads <= 1;
	drain_task_count = single_collection_drain ? 1 : MinValue(max_threads, scheduler_threads);
	expected_partition_count = single_collection_drain ? 1 : max_threads;
	drain_partitions.reserve(expected_partition_count);
	drain_writers.reserve(drain_task_count);
}

void AdaptiveSortedWrite::FinishParallelDrain(ClientContext &context) {
	{
		lock_guard<mutex> guard(lock);
		if (phase != AdaptiveInsertPhase::DRAINING || drained_row_count != total_count ||
		    drain_partitions.size() != expected_partition_count) {
			throw InternalException("Adaptive sorted write did not drain every sorted partition");
		}
	}

	std::sort(drain_partitions.begin(), drain_partitions.end(),
	          [](const DrainPartition &left, const DrainPartition &right) {
		          return left.partition_index < right.partition_index;
	          });
	auto &storage = table.GetStorage();
	idx_t merged_count = 0;
	for (idx_t partition_index = 0; partition_index < drain_partitions.size(); partition_index++) {
		auto &partition = drain_partitions[partition_index];
		if (partition.partition_index != partition_index) {
			throw InternalException("Adaptive sorted write produced a duplicate or missing output partition");
		}
		auto &collection = storage.GetOptimisticCollection(context, partition.collection_index);
		if (collection.collection->GetTotalRows() != partition.row_count) {
			throw InternalException("Adaptive sorted write partition row count changed before merge");
		}
		storage.LocalMerge(context, table, collection);
		storage.ResetOptimisticCollection(context, partition.collection_index);
		merged_count += partition.row_count;
	}
	if (merged_count != total_count) {
		throw InternalException("Adaptive sorted write merged an invalid row count");
	}

	auto &target_writer = storage.GetOptimisticWriter(context);
	for (auto &writer : drain_writers) {
		target_writer.Merge(*writer);
	}
	target_writer.FinalFlush();
	drain_writers.clear();
	drain_partitions.clear();
	MarkFinished();
}

void AdaptiveSortedWrite::RollbackDrainWriters() noexcept {
	for (auto &writer : drain_writers) {
		try {
			writer->Rollback();
		} catch (...) {
		}
	}
	drain_writers.clear();
}

void AdaptiveSortedWrite::MarkFinished() {
	{
		lock_guard<mutex> guard(lock);
		if (phase != AdaptiveInsertPhase::DRAINING) {
			throw InternalException("Adaptive sorted write finished from an invalid phase");
		}
		phase = AdaptiveInsertPhase::FINISHED;
	}
	phase_changed.notify_all();
}

class AdaptiveSortedWriteDrainTask : public ExecutorTask {
public:
	AdaptiveSortedWriteDrainTask(Pipeline &pipeline_p, shared_ptr<Event> event_p, const PhysicalOperator &op,
	                             AdaptiveSortedWrite &write_p)
	    : ExecutorTask(pipeline_p.GetClientContext(), std::move(event_p), op), pipeline(pipeline_p), write(write_p),
	      initialized(false), collection_index(DConstants::INVALID_INDEX), partition_row_count(0) {
	}

	void FinalizePartition(ExecutionContext &execution) {
		if (!partition_index.IsValid()) {
			return;
		}
		auto &storage = write.table.GetStorage();
		auto &collection = storage.GetOptimisticCollection(execution.client, collection_index);
		collection.FinalizeAppend(TransactionData(0, 0), append_state);
		writer->WriteUnflushedRowGroups(collection);
		write.RegisterDrainPartition(partition_index.GetIndex(), collection_index, partition_row_count);
		partition_index = optional_idx();
		collection_index = PhysicalIndex(DConstants::INVALID_INDEX);
		partition_row_count = 0;
	}

	void InitializePartition(ExecutionContext &execution, idx_t new_partition_index) {
		if (!writer) {
			writer = make_uniq<OptimisticDataWriter>(execution.client, write.table.GetStorage());
		}
		auto collection = writer->CreateCollection(write.table.GetStorage(), write.input_types);
		collection->collection->InitializeEmpty();
		collection->InitializeAppend(append_state,
		                             AppendOrganization::Sorted(write.sort_definition.sort_order_id, write.run_id));
		collection_index = write.CreateDrainCollection(execution.client, std::move(collection));
		partition_index = new_partition_index;
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		(void)mode;
		try {
			ExecutionContext execution(pipeline.GetClientContext(), *thread_context, &pipeline);
			if (!initialized) {
				local_source = write.sort->GetLocalSourceState(execution, *write.sort_source);
				output.Initialize(execution.client, write.input_types);
				initialized = true;
			}

			InterruptState interrupt_state {weak_ptr<Task>(shared_from_this())};
			OperatorSourceInput sort_input {*write.sort_source, *local_source, interrupt_state};
			output.Reset();
			auto result = write.sort->GetData(execution, output, sort_input);
			if (result == SourceResultType::BLOCKED) {
				return TaskExecutionResult::TASK_BLOCKED;
			}
			if (result == SourceResultType::FINISHED) {
				FinalizePartition(execution);
				write.RegisterDrainWriter(std::move(writer));
				event->FinishTask();
				return TaskExecutionResult::TASK_FINISHED;
			}
			auto partition = write.single_collection_drain
			                     ? 0
			                     : write.sort
			                           ->GetPartitionData(execution, output, *write.sort_source, *local_source,
			                                              OperatorPartitionInfo())
			                           .batch_index;
			if (!partition_index.IsValid() || partition_index.GetIndex() != partition) {
				FinalizePartition(execution);
				InitializePartition(execution, partition);
			}
			auto &collection = write.table.GetStorage().GetOptimisticCollection(execution.client, collection_index);
			auto flushed_row_group = collection.Append(output, append_state);
			if (flushed_row_group.IsValid()) {
				writer->WriteNewRowGroup(collection, flushed_row_group.GetIndex());
			}
			partition_row_count += output.size();
			return TaskExecutionResult::TASK_NOT_FINISHED;
		} catch (...) {
			if (writer) {
				writer->Rollback();
			}
			write.SetFailure(std::current_exception());
			throw;
		}
	}

	string TaskType() const override {
		return "AdaptiveSortedWriteDrainTask";
	}

private:
	Pipeline &pipeline;
	AdaptiveSortedWrite &write;
	bool initialized;
	unique_ptr<LocalSourceState> local_source;
	DataChunk output;
	TableAppendState append_state;
	unique_ptr<OptimisticDataWriter> writer;
	optional_idx partition_index;
	PhysicalIndex collection_index;
	idx_t partition_row_count;
};

class AdaptiveSortedWriteDrainEvent : public BasePipelineEvent {
public:
	AdaptiveSortedWriteDrainEvent(Pipeline &pipeline_p, const PhysicalOperator &op_p, AdaptiveSortedWrite &write_p)
	    : BasePipelineEvent(pipeline_p), op(op_p), write(write_p) {
	}

	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		for (idx_t task_index = 0; task_index < write.drain_task_count; task_index++) {
			tasks.push_back(make_uniq<AdaptiveSortedWriteDrainTask>(*pipeline, shared_from_this(), op, write));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		try {
			write.FinishParallelDrain(GetClientContext());
		} catch (...) {
			write.SetFailure(std::current_exception());
			throw;
		}
	}

private:
	const PhysicalOperator &op;
	AdaptiveSortedWrite &write;
};

SinkFinalizeType AdaptiveSortedWrite::FinalizeInternal(Pipeline &pipeline, Event &event, const PhysicalOperator &op,
                                                       ClientContext &context, InterruptState &interrupt_state) {
	OrderedInsertStaging owned_staging;
	bool needs_sort_drain = false;
	{
		unique_lock<mutex> guard(lock);
		while (phase == AdaptiveInsertPhase::INITIALIZING_SORT) {
			phase_changed.wait(guard);
		}
		if (phase == AdaptiveInsertPhase::FAILED) {
			ThrowFailure(guard);
		}
		if (phase == AdaptiveInsertPhase::BUFFERING) {
			D_ASSERT(staged_count == total_count);
			owned_staging = std::move(staging);
			phase = AdaptiveInsertPhase::DRAINING;
		} else if (phase == AdaptiveInsertPhase::SORTING) {
			phase = AdaptiveInsertPhase::DRAINING;
			needs_sort_drain = true;
		} else {
			throw InternalException("Adaptive sorted write finalized more than once");
		}
	}

	if (!needs_sort_drain) {
		DrainUnsorted(context, std::move(owned_staging));
		MarkFinished();
		return SinkFinalizeType::READY;
	}

	OperatorSinkFinalizeInput sort_input {*sort_sink, interrupt_state};
	auto result = sort->Finalize(context, sort_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("Adaptive sorted write sorter produced no output after reaching its threshold");
	}
	sort_source = sort->GetGlobalSourceState(context, *sort_sink);
	PrepareParallelDrain(context);
	auto drain_event = make_shared_ptr<AdaptiveSortedWriteDrainEvent>(pipeline, op, *this);
	event.InsertEvent(std::move(drain_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType AdaptiveSortedWrite::Finalize(Pipeline &pipeline, Event &event, const PhysicalOperator &op,
                                               ClientContext &context, InterruptState &interrupt_state) {
	try {
		return FinalizeInternal(pipeline, event, op, context, interrupt_state);
	} catch (...) {
		SetFailure(std::current_exception());
		throw;
	}
}

idx_t AdaptiveSortedWrite::TotalCount() const {
	lock_guard<mutex> guard(lock);
	return total_count;
}

AdaptiveInsertPhase AdaptiveSortedWrite::GetPhase() const {
	lock_guard<mutex> guard(lock);
	return phase;
}

sort_run_id_t AdaptiveSortedWrite::GetRunId() const {
	lock_guard<mutex> guard(lock);
	return run_id;
}

} // namespace duckdb
