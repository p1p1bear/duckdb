#include "duckdb/storage/recluster/recluster_output_writer.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_sorter.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {

class ReclusterRowGroupWriteTask : public BaseExecutorTask {
public:
	ReclusterRowGroupWriteTask(TaskExecutor &executor, RangeTask &task_p, const RowGroup &row_group_p,
	                           PartialBlockManager &partial_manager_p,
	                           const vector<CompressionType> &compression_types_p, RowGroupWriteData &result_p)
	    : BaseExecutorTask(executor), task(task_p), row_group(row_group_p), partial_manager(partial_manager_p),
	      compression_types(compression_types_p), result(result_p) {
	}

	void ExecuteTask() override {
		if (task.IsCancelRequested() || task.IsPublishForbidden()) {
			throw InterruptException("Recluster task was cancelled");
		}
		task.GetTaskContext().InterruptCheck();
		RowGroupWriteInfo write_info(partial_manager, compression_types);
		result = row_group.WriteToDisk(write_info);
		partial_manager.FlushPartialBlocks();
	}

	string TaskType() const override {
		return "ReclusterRowGroupWriteTask";
	}

private:
	RangeTask &task;
	const RowGroup &row_group;
	PartialBlockManager &partial_manager;
	const vector<CompressionType> &compression_types;
	RowGroupWriteData &result;
};

ReclusterOutput::ReclusterOutput(BlockManager &block_manager_p, shared_ptr<RowGroupCollection> collection_p,
                                 unique_ptr<PersistentCollectionData> persistent_data_p,
                                 sort_order_id_t sort_order_id_p, sort_run_id_t run_id_p, idx_t row_count_p)
    : block_manager(block_manager_p), collection(std::move(collection_p)),
      persistent_data(std::move(persistent_data_p)), sort_order_id(sort_order_id_p), run_id(run_id_p),
      row_count(row_count_p) {
	if (!collection || !persistent_data || sort_order_id == INVALID_SORT_ORDER_ID || run_id == INVALID_SORT_RUN_ID ||
	    collection->GetTotalRows() != row_count) {
		throw InternalException("Invalid recluster private output");
	}
}

ReclusterOutput::~ReclusterOutput() {
	if (!owns_blocks) {
		return;
	}
	try {
		Abort();
	} catch (...) { // NOLINT: destructors cannot report asynchronous cleanup failures
	}
}

const PersistentCollectionData &ReclusterOutput::GetPersistentData() const {
	return *persistent_data;
}

vector<shared_ptr<RowGroup>> ReclusterOutput::GetRowGroups() const {
	vector<shared_ptr<RowGroup>> result;
	result.reserve(collection->GetRowGroupCount());
	for (idx_t row_group_index = 0; row_group_index < collection->GetRowGroupCount(); row_group_index++) {
		auto row_group = collection->GetRowGroups()->GetSegmentByIndex(NumericCast<int64_t>(row_group_index));
		if (!row_group) {
			throw InternalException("Missing recluster output row group");
		}
		result.push_back(row_group->ReferenceNode());
	}
	return result;
}

void ReclusterOutput::AdoptTaskPrivateBlocks(vector<block_id_t> block_ids_p) {
	if (owns_blocks || !block_ids.empty()) {
		throw InternalException("Recluster output already owns task-private blocks");
	}
	block_ids = std::move(block_ids_p);
	owns_blocks = true;
}

void ReclusterOutput::MarkPublished() {
	if (!owns_blocks) {
		throw InternalException("Cannot publish recluster output without private block ownership");
	}
	owns_blocks = false;
}

void ReclusterOutput::Abort() {
	if (!owns_blocks) {
		return;
	}
	for (auto block_id : block_ids) {
		block_manager.MarkBlockAsModified(block_id);
	}
	owns_blocks = false;
}

ReclusterOutputWriter::ReclusterOutputWriter(RangeTask &task_p) : task(task_p) {
	if (task.GetState() != RangeTaskState::PREPARING || !task.HasTaskContext()) {
		throw InternalException("Recluster output writer requires a preparing task");
	}
	if (task.GetTaskContext().HasOutput()) {
		throw InternalException("Recluster task already has private output");
	}
}

void ReclusterOutputWriter::CheckTask() const {
	if (task.IsCancelRequested() || task.IsPublishForbidden()) {
		throw InterruptException("Recluster task was cancelled");
	}
	if (task.GetState() != RangeTaskState::PREPARING) {
		throw InternalException("Recluster output writer observed an invalid task state");
	}
}

static vector<column_t> ReclusterPhysicalColumns(idx_t column_count) {
	vector<column_t> result;
	result.reserve(column_count);
	for (idx_t column_index = 0; column_index < column_count; column_index++) {
		result.push_back(NumericCast<column_t>(column_index));
	}
	return result;
}

static vector<block_id_t> UniqueSortedBlocks(vector<block_id_t> blocks) {
	std::sort(blocks.begin(), blocks.end());
	blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
	return blocks;
}

void ReclusterOutputWriter::Write() {
	CheckTask();
	auto &task_context = task.GetTaskContext();
	if (task_context.HasOutput()) {
		throw InternalException("Recluster task already has private output");
	}
	auto &storage = *task_context.GetStorage();
	auto types = storage.GetTypes();
	auto &range = task.GetRange();
	auto collection = make_shared_ptr<RowGroupCollection>(storage.GetDataTableInfo(), storage.GetTableIOManager(),
	                                                      types, NumericCast<idx_t>(range.start));
	collection->InitializeEmpty();

	auto run_id = storage.GetDataTableInfo()->GetSortStorage().AllocateRunId();
	auto organization = AppendOrganization::Sorted(task_context.GetSortDefinition().sort_order_id, run_id);
	ReclusterSorter sorter(task);
	sorter.Prepare();

	TableAppendState append_state;
	bool append_initialized = false;
	DataChunk sorted_chunk;
	sorter.InitializeChunk(sorted_chunk);
	auto physical_columns = ReclusterPhysicalColumns(types.size());
	while (sorter.Scan(sorted_chunk)) {
		CheckTask();
		DataChunk table_chunk;
		table_chunk.InitializeEmpty(types);
		table_chunk.ReferenceColumns(sorted_chunk, physical_columns);
		if (!append_initialized) {
			collection->InitializeAppend(TransactionData(0, 0), append_state, organization);
			append_initialized = true;
		}
		collection->Append(table_chunk, append_state);
	}
	if (append_initialized) {
		collection->FinalizeAppend(TransactionData(0, 0), append_state);
	}
	if (collection->GetTotalRows() != sorter.GetSortedRowCount()) {
		throw InternalException("Recluster private output row count does not match sorted input");
	}
	CheckTask();

	auto &block_manager = storage.GetTableIOManager().GetBlockManagerForRowData();
	vector<unique_ptr<PartialBlockManager>> partial_managers;
	try {
		vector<CompressionType> compression_types;
		compression_types.reserve(storage.Columns().size());
		for (auto &column : storage.Columns()) {
			compression_types.push_back(column.CompressionType());
		}
		if (compression_types.size() != types.size()) {
			throw InternalException("Recluster output physical column count changed during Prepare");
		}

		vector<const_reference<RowGroup>> row_groups;
		vector<int64_t> row_group_indexes;
		row_groups.reserve(collection->GetRowGroupCount());
		row_group_indexes.reserve(collection->GetRowGroupCount());
		for (idx_t row_group_index = 0; row_group_index < collection->GetRowGroupCount(); row_group_index++) {
			auto index = NumericCast<int64_t>(row_group_index);
			auto row_group = collection->GetRowGroup(index);
			if (!row_group) {
				throw InternalException("Missing in-memory recluster output row group");
			}
			row_groups.push_back(*row_group);
			row_group_indexes.push_back(index);
		}

		vector<RowGroupWriteData> write_data(row_groups.size());
		partial_managers.reserve(row_groups.size());
		auto &scheduler = TaskScheduler::GetScheduler(storage.GetAttached().GetDatabase());
		TaskExecutor executor(scheduler);
		for (idx_t row_group_index = 0; row_group_index < row_groups.size(); row_group_index++) {
			partial_managers.push_back(make_uniq<PartialBlockManager>(
			    QueryContext(), block_manager, PartialBlockType::RECLUSTER_TASK, optional_idx(0), 1));
			executor.ScheduleTask(make_uniq<ReclusterRowGroupWriteTask>(executor, task, row_groups[row_group_index],
			                                                            *partial_managers.back(), compression_types,
			                                                            write_data[row_group_index]));
		}
		executor.WorkOnTasks();
		if (write_data.size() != row_group_indexes.size()) {
			throw InternalException("Recluster output checkpoint returned an invalid row group count");
		}
		for (idx_t row_group_index = 0; row_group_index < write_data.size(); row_group_index++) {
			collection->SetRowGroup(row_group_indexes[row_group_index],
			                        std::move(write_data[row_group_index].result_row_group));
		}
		write_data.clear();
		collection->Verify();
		CheckTask();

		auto persistent_data = make_uniq<PersistentCollectionData>();
		auto row_start = range.start;
		for (idx_t row_group_index = 0; row_group_index < collection->GetRowGroupCount(); row_group_index++) {
			auto row_group = collection->GetRowGroup(NumericCast<int64_t>(row_group_index));
			if (!row_group || !row_group->IsPersistent()) {
				throw InternalException("Recluster output row group was not persisted");
			}
			auto row_group_data = row_group->SerializeRowGroupInfo(NumericCast<idx_t>(row_start));
			row_group_data.types = types;
			row_start += NumericCast<row_t>(row_group->count.load());
			persistent_data->row_group_data.push_back(std::move(row_group_data));
		}
		if (row_start != range.start + NumericCast<row_t>(collection->GetTotalRows()) || row_start > range.end) {
			throw InternalException("Recluster output row IDs exceed the candidate range");
		}

		auto referenced_blocks = UniqueSortedBlocks(persistent_data->GetBlockIds());
		block_manager.FileSync();
		CheckTask();
		auto output = unique_ptr<ReclusterOutput>(
		    new ReclusterOutput(block_manager, collection, std::move(persistent_data), organization.sort_order_id,
		                        run_id, collection->GetTotalRows()));
		vector<block_id_t> task_blocks;
		for (auto &partial_manager : partial_managers) {
			auto manager_blocks = partial_manager->TakeTaskPrivateBlocks();
			task_blocks.insert(task_blocks.end(), manager_blocks.begin(), manager_blocks.end());
		}
		task_blocks = UniqueSortedBlocks(std::move(task_blocks));
		output->AdoptTaskPrivateBlocks(std::move(task_blocks));
		if (output->GetBlockIds() != referenced_blocks) {
			throw InternalException("Recluster task-private block ownership does not match its persistent output");
		}
		task_context.SetOutput(std::move(output));
	} catch (...) {
		for (auto &partial_manager : partial_managers) {
			partial_manager->Rollback();
		}
		throw;
	}
}

} // namespace duckdb
