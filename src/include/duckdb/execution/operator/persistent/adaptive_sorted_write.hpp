//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/adaptive_sorted_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

#include <exception>
#include <map>

namespace duckdb {

class BoundConstraint;
class DuckTableEntry;
class Event;
class PhysicalOperator;
class Pipeline;
class Sort;
class OptimisticDataWriter;
struct OptimisticWriteCollection;

enum class AdaptiveInsertPhase : uint8_t { BUFFERING, SORTING, DRAINING, FINISHED, FAILED };

struct InsertOrderToken {
	optional_idx batch_index;
	idx_t chunk_index = 0;

	static InsertOrderToken ArrivalOrder();
	static InsertOrderToken BatchOrder(idx_t batch_index, idx_t chunk_index);

	bool operator<(const InsertOrderToken &other) const;
};

struct AdaptiveSortedWriteLocalState {
	unique_ptr<LocalSinkState> sort_sink;
};

class AdaptiveSortedWrite {
	friend class AdaptiveSortedWriteDrainTask;
	friend class AdaptiveSortedWriteDrainEvent;

public:
	AdaptiveSortedWrite(DuckTableEntry &table, vector<LogicalType> input_types,
	                    const vector<unique_ptr<BoundConstraint>> &bound_constraints);
	~AdaptiveSortedWrite();

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, AdaptiveSortedWriteLocalState &local_state,
	                    const InsertOrderToken &order_token, InterruptState &interrupt_state);
	SinkCombineResultType Combine(ExecutionContext &context, AdaptiveSortedWriteLocalState &local_state,
	                              InterruptState &interrupt_state);
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, const PhysicalOperator &op, ClientContext &context,
	                          InterruptState &interrupt_state);

	idx_t TotalCount() const;

private:
	using OrderedInsertStaging = std::map<InsertOrderToken, unique_ptr<DataChunk>>;
	enum class InsertOrderMode : uint8_t { UNSET, ARRIVAL, BATCH };
	struct DrainPartition {
		idx_t partition_index;
		PhysicalIndex collection_index;
		idx_t row_count;
	};

	SinkResultType SinkInternal(ExecutionContext &context, DataChunk &chunk, AdaptiveSortedWriteLocalState &local_state,
	                            const InsertOrderToken &order_token, InterruptState &interrupt_state);
	SinkCombineResultType CombineInternal(ExecutionContext &context, AdaptiveSortedWriteLocalState &local_state,
	                                      InterruptState &interrupt_state);
	SinkFinalizeType FinalizeInternal(Pipeline &pipeline, Event &event, const PhysicalOperator &op,
	                                  ClientContext &context, InterruptState &interrupt_state);

	InsertOrderToken ResolveOrderToken(const InsertOrderToken &order_token);
	void EnsureWriteGate(ClientContext &context);
	void AppendOwned(ClientContext &context, DataChunk &chunk, idx_t offset, idx_t count,
	                 const InsertOrderToken &order_token);
	void InitializeSort(ClientContext &context);
	void SinkToSort(ExecutionContext &context, DataChunk &chunk, AdaptiveSortedWriteLocalState &local_state,
	                InterruptState &interrupt_state);
	void DrainUnsorted(ClientContext &context, OrderedInsertStaging staging);
	PhysicalIndex CreateDrainCollection(ClientContext &context, unique_ptr<OptimisticWriteCollection> collection);
	void RegisterDrainPartition(idx_t partition_index, PhysicalIndex collection_index, idx_t row_count);
	void RegisterDrainWriter(unique_ptr<OptimisticDataWriter> writer);
	void PrepareParallelDrain(ClientContext &context);
	void FinishParallelDrain(ClientContext &context);
	void RollbackDrainWriters() noexcept;
	void MarkFinished();
	void SetFailure(std::exception_ptr exception);
	void ThrowFailure(unique_lock<mutex> &guard) const;

private:
	DuckTableEntry &table;
	vector<LogicalType> input_types;
	const vector<unique_ptr<BoundConstraint>> &bound_constraints;
	SortOrderDefinition sort_definition;

	mutable mutex lock;
	AdaptiveInsertPhase phase = AdaptiveInsertPhase::BUFFERING;
	idx_t total_count = 0;
	idx_t row_group_size;
	idx_t next_arrival_token = 0;
	InsertOrderMode order_mode = InsertOrderMode::UNSET;
	bool write_gate_held = false;
	sort_run_id_t run_id = INVALID_SORT_RUN_ID;
	OrderedInsertStaging staging;
	unique_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sort_sink;
	unique_ptr<GlobalSourceState> sort_source;
	idx_t drain_task_count = 0;
	idx_t expected_partition_count = 0;
	bool single_collection_drain = false;
	vector<DrainPartition> drain_partitions;
	vector<unique_ptr<OptimisticDataWriter>> drain_writers;
	std::exception_ptr failure;
};

} // namespace duckdb
