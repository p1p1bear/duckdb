#include "duckdb/storage/recluster/recluster_output_writer.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
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
#include "duckdb/storage/table/row_version_manager.hpp"
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

static vector<block_id_t> UniqueSortedBlocks(vector<block_id_t> blocks) {
	std::sort(blocks.begin(), blocks.end());
	blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
	return blocks;
}

class ReclusterTaskMetadataWriter : public MetadataWriter {
public:
	explicit ReclusterTaskMetadataWriter(TaskPrivateMetadataBlockOwner &owner_p)
	    : MetadataWriter(owner_p.GetManager()), owner(owner_p) {
	}

protected:
	MetadataHandle NextHandle() override {
		return owner.AllocateHandle();
	}

private:
	TaskPrivateMetadataBlockOwner &owner;
};

ReclusterOutput::ReclusterOutput(BlockManager &block_manager_p, shared_ptr<RowGroupCollection> collection_p,
                                 unique_ptr<PersistentCollectionData> persistent_data_p,
                                 sort_order_id_t sort_order_id_p, sort_run_id_t run_id_p, idx_t row_count_p,
                                 unique_ptr<TaskPrivateMetadataBlockOwner> replacement_metadata_owner_p,
                                 unique_ptr<TaskPrivateMetadataBlockOwner> manifest_owner_p,
                                 ReplacementManifest manifest_p, MetaBlockPointer manifest_pointer_p)
    : block_manager(block_manager_p), collection(std::move(collection_p)),
      persistent_data(std::move(persistent_data_p)),
      replacement_metadata_owner(std::move(replacement_metadata_owner_p)), manifest_owner(std::move(manifest_owner_p)),
      manifest(std::move(manifest_p)), manifest_pointer(manifest_pointer_p), sort_order_id(sort_order_id_p),
      run_id(run_id_p), row_count(row_count_p) {
	if (!collection || !persistent_data || sort_order_id == INVALID_SORT_ORDER_ID || run_id == INVALID_SORT_RUN_ID ||
	    collection->GetTotalRows() != row_count || !replacement_metadata_owner || !manifest_owner ||
	    !manifest_pointer.IsValid()) {
		throw InternalException("Invalid recluster private output");
	}
	manifest.Validate();
	auto manifest_blocks = manifest_owner->GetBlockIds();
	if (!std::binary_search(manifest_blocks.begin(), manifest_blocks.end(), manifest_pointer.GetBlockId())) {
		throw InternalException("Recluster manifest pointer is not owned by its metadata chain");
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
	if (owns_blocks || !data_block_ids.empty() || !block_ids.empty()) {
		throw InternalException("Recluster output already owns task-private blocks");
	}
	data_block_ids = std::move(block_ids_p);
	owns_blocks = true;

	auto expected_referenced_blocks = GetReferencedBlockIds();
	if (expected_referenced_blocks != manifest.all_referenced_blocks) {
		throw InternalException("Recluster replacement block ownership does not match its manifest");
	}
	RefreshBlockIds();
}

vector<block_id_t> ReclusterOutput::GetReferencedBlockIds() const {
	vector<block_id_t> result = data_block_ids;
	auto replacement_metadata_blocks = replacement_metadata_owner->GetBlockIds();
	result.insert(result.end(), replacement_metadata_blocks.begin(), replacement_metadata_blocks.end());
	if (delete_metadata_owner) {
		auto delete_metadata_blocks = delete_metadata_owner->GetBlockIds();
		result.insert(result.end(), delete_metadata_blocks.begin(), delete_metadata_blocks.end());
	}
	return UniqueSortedBlocks(std::move(result));
}

void ReclusterOutput::RefreshBlockIds() {
	block_ids = GetReferencedBlockIds();
	auto manifest_blocks = manifest_owner->GetBlockIds();
	block_ids.insert(block_ids.end(), manifest_blocks.begin(), manifest_blocks.end());
	block_ids = UniqueSortedBlocks(std::move(block_ids));
}

idx_t ReclusterOutput::ApplyCommittedDeletes(const vector<row_t> &new_rowids) {
	auto replacement_start = manifest.header.input_range.start;
	auto replacement_end = replacement_start + NumericCast<row_t>(row_count);
	auto row_groups = collection->GetRowGroups();
	idx_t deleted_count = 0;
	idx_t row_index = 0;
	while (row_index < new_rowids.size()) {
		auto new_rowid = new_rowids[row_index];
		if (new_rowid < replacement_start || new_rowid >= replacement_end) {
			throw InternalException("Recluster DELETE remap points outside the replacement output");
		}
		auto row_group = row_groups->GetSegment(NumericCast<idx_t>(new_rowid));
		if (!row_group) {
			throw InternalException("Recluster DELETE remap points to a missing replacement row group");
		}
		auto row_group_start = NumericCast<row_t>(row_group->GetRowStart());
		auto row_group_end = NumericCast<row_t>(row_group->GetRowEnd());
		auto vector_index = NumericCast<idx_t>((new_rowid - row_group_start) / STANDARD_VECTOR_SIZE);
		auto vector_start = row_group_start + NumericCast<row_t>(vector_index * STANDARD_VECTOR_SIZE);
		auto vector_end = MinValue<row_t>(row_group_end, vector_start + NumericCast<row_t>(STANDARD_VECTOR_SIZE));

		row_t vector_offsets[STANDARD_VECTOR_SIZE];
		idx_t vector_count = 0;
		while (row_index < new_rowids.size() && new_rowids[row_index] < vector_end) {
			if (new_rowids[row_index] < vector_start) {
				throw InternalException("Recluster DELETE remap row IDs are not monotonic");
			}
			vector_offsets[vector_count++] = new_rowids[row_index] - vector_start;
			row_index++;
		}
		deleted_count += row_group->GetNode().GetOrCreateVersionInfo().DeleteCommittedRows(vector_index, vector_offsets,
		                                                                                   vector_count);
	}
	return deleted_count;
}

idx_t ReclusterOutput::ApplyFinalDeletes(const vector<row_t> &new_rowids) {
	if (!owns_blocks) {
		throw InternalException("Cannot apply final DELETEs without private block ownership");
	}
	return ApplyCommittedDeletes(new_rowids);
}

idx_t ReclusterOutput::ApplyDeleteCatchup(vector<row_t> new_rowids, delete_sequence_t resolved_through) {
	if (!owns_blocks || resolved_through <= manifest.header.last_applied_delete_sequence) {
		throw InternalException("Invalid recluster DELETE catch-up sequence");
	}
	if (manifest.header.manifest_revision == NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("Recluster replacement manifest revision space is exhausted");
	}

	std::sort(new_rowids.begin(), new_rowids.end());
	new_rowids.erase(std::unique(new_rowids.begin(), new_rowids.end()), new_rowids.end());
	auto deleted_count = ApplyCommittedDeletes(new_rowids);

	auto &metadata_manager = block_manager.GetMetadataManager();
	auto next_delete_owner = metadata_manager.CreateTaskPrivateBlockOwner();
	ReplacementManifest next_manifest = manifest;
	{
		ReclusterTaskMetadataWriter delete_writer(*next_delete_owner);
		if (next_manifest.replacement_groups.size() != collection->GetRowGroupCount()) {
			throw InternalException("Recluster replacement manifest row groups changed during DELETE catch-up");
		}
		for (idx_t row_group_index = 0; row_group_index < collection->GetRowGroupCount(); row_group_index++) {
			auto row_group = collection->GetRowGroup(NumericCast<int64_t>(row_group_index));
			if (!row_group) {
				throw InternalException("Missing recluster replacement row group during DELETE catch-up");
			}
			next_manifest.replacement_groups[row_group_index].deletes_pointers =
			    row_group->GetOrCreateVersionInfo().Checkpoint(delete_writer, 0);
		}
		delete_writer.Flush();
	}
	next_delete_owner->Flush();
	if (next_delete_owner->GetBlockIds().empty()) {
		next_delete_owner->Abort();
		next_delete_owner.reset();
	}

	next_manifest.header.last_applied_delete_sequence = resolved_through;
	next_manifest.header.manifest_revision++;
	next_manifest.all_referenced_blocks = data_block_ids;
	auto replacement_metadata_blocks = replacement_metadata_owner->GetBlockIds();
	next_manifest.all_referenced_blocks.insert(next_manifest.all_referenced_blocks.end(),
	                                           replacement_metadata_blocks.begin(), replacement_metadata_blocks.end());
	if (next_delete_owner) {
		auto delete_metadata_blocks = next_delete_owner->GetBlockIds();
		next_manifest.all_referenced_blocks.insert(next_manifest.all_referenced_blocks.end(),
		                                           delete_metadata_blocks.begin(), delete_metadata_blocks.end());
	}
	next_manifest.all_referenced_blocks = UniqueSortedBlocks(std::move(next_manifest.all_referenced_blocks));
	next_manifest.Seal();

	auto next_manifest_owner = metadata_manager.CreateTaskPrivateBlockOwner();
	MetaBlockPointer next_manifest_pointer;
	{
		ReclusterTaskMetadataWriter manifest_writer(*next_manifest_owner);
		next_manifest_pointer = manifest_writer.GetMetaBlockPointer();
		next_manifest.Write(manifest_writer);
		manifest_writer.Flush();
	}
	next_manifest_owner->Flush();
	block_manager.FileSync();

	auto previous_delete_owner = std::move(delete_metadata_owner);
	auto previous_manifest_owner = std::move(manifest_owner);
	delete_metadata_owner = std::move(next_delete_owner);
	manifest_owner = std::move(next_manifest_owner);
	manifest = std::move(next_manifest);
	manifest_pointer = next_manifest_pointer;
	RefreshBlockIds();
	if (previous_delete_owner) {
		previous_delete_owner->Abort();
	}
	previous_manifest_owner->Abort();
	return deleted_count;
}

void ReclusterOutput::MarkPublished() {
	if (!owns_blocks) {
		throw InternalException("Cannot publish recluster output without private block ownership");
	}
	for (auto block_id : data_block_ids) {
		block_manager.MarkBlockAsCheckpointed(block_id);
	}
	replacement_metadata_owner->MarkPublished();
	if (delete_metadata_owner) {
		delete_metadata_owner->MarkPublished();
	}
	manifest_owner->MarkPublished();
	owns_blocks = false;
}

void ReclusterOutput::Abort() {
	if (!owns_blocks) {
		return;
	}
	for (auto block_id : data_block_ids) {
		block_manager.MarkBlockAsModified(block_id);
	}
	replacement_metadata_owner->Abort();
	if (delete_metadata_owner) {
		delete_metadata_owner->Abort();
	}
	manifest_owner->Abort();
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

		auto data_blocks = UniqueSortedBlocks(persistent_data->GetBlockIds());
		auto &metadata_manager = block_manager.GetMetadataManager();
		auto replacement_metadata_owner = metadata_manager.CreateTaskPrivateBlockOwner();
		vector<RowGroupPointer> replacement_groups;
		replacement_groups.reserve(persistent_data->row_group_data.size());
		{
			ReclusterTaskMetadataWriter metadata_writer(*replacement_metadata_owner);
			for (auto &row_group_data : persistent_data->row_group_data) {
				CheckTask();
				if (row_group_data.column_data.size() != types.size() || row_group_data.types != types ||
				    row_group_data.count == 0) {
					throw InternalException("Invalid persistent recluster replacement row group");
				}

				RowGroupPointer pointer;
				pointer.row_start = row_group_data.start;
				pointer.tuple_count = row_group_data.count;
				pointer.has_per_column_metadata_blocks = true;
				pointer.sort_metadata = {organization.sort_order_id, run_id};
				for (idx_t column_index = 0; column_index < row_group_data.column_data.size(); column_index++) {
					vector<MetaBlockPointer> column_written_blocks;
					metadata_writer.SetWrittenPointers(column_written_blocks);
					auto column_pointer = metadata_writer.GetMetaBlockPointer();
					pointer.data_pointers.push_back(column_pointer);

					BinarySerializer serializer(metadata_writer,
					                            SerializationOptions(storage.GetDataTableInfo()->GetDB()));
					serializer.Begin();
					row_group_data.column_data[column_index].Serialize(serializer);
					serializer.End();
					metadata_writer.SetWrittenPointers(nullptr);

					vector<idx_t> extra_blocks;
					for (auto &written_pointer : column_written_blocks) {
						if (written_pointer.block_pointer != column_pointer.block_pointer) {
							extra_blocks.push_back(written_pointer.block_pointer);
						}
					}
					pointer.per_column_metadata_blocks.AddColumn(column_index, std::move(extra_blocks));
				}
				replacement_groups.push_back(std::move(pointer));
			}
			metadata_writer.Flush();
		}
		replacement_metadata_owner->Flush();
		CheckTask();

		ReplacementManifest manifest;
		manifest.header.task_id = task.GetTaskId();
		manifest.header.table_id = task_context.GetTableId();
		manifest.header.prepared_layout_version = task_context.GetCandidate().layout_version;
		manifest.header.sort_order_id = organization.sort_order_id;
		manifest.header.run_id = run_id;
		manifest.header.input_range = range;
		manifest.sort_columns = task_context.GetSortDefinition().columns;
		manifest.old_groups = task_context.GetCandidate().expected_row_groups;
		manifest.replacement_groups = std::move(replacement_groups);
		manifest.physical_columns.reserve(storage.Columns().size());
		for (auto &column : storage.Columns()) {
			manifest.physical_columns.push_back({column.PersistentColumnId(), column.Type()});
		}
		manifest.all_referenced_blocks = data_blocks;
		auto replacement_metadata_blocks = replacement_metadata_owner->GetBlockIds();
		manifest.all_referenced_blocks.insert(manifest.all_referenced_blocks.end(), replacement_metadata_blocks.begin(),
		                                      replacement_metadata_blocks.end());
		manifest.all_referenced_blocks = UniqueSortedBlocks(std::move(manifest.all_referenced_blocks));
		manifest.Seal();

		auto manifest_owner = metadata_manager.CreateTaskPrivateBlockOwner();
		MetaBlockPointer manifest_pointer;
		{
			ReclusterTaskMetadataWriter manifest_writer(*manifest_owner);
			manifest_pointer = manifest_writer.GetMetaBlockPointer();
			manifest.Write(manifest_writer);
			manifest_writer.Flush();
		}
		manifest_owner->Flush();
		block_manager.FileSync();
		CheckTask();
		auto output = unique_ptr<ReclusterOutput>(
		    new ReclusterOutput(block_manager, collection, std::move(persistent_data), organization.sort_order_id,
		                        run_id, collection->GetTotalRows(), std::move(replacement_metadata_owner),
		                        std::move(manifest_owner), std::move(manifest), manifest_pointer));
		vector<block_id_t> task_blocks;
		for (auto &partial_manager : partial_managers) {
			auto manager_blocks = partial_manager->TakeTaskPrivateBlocks();
			task_blocks.insert(task_blocks.end(), manager_blocks.begin(), manager_blocks.end());
		}
		task_blocks = UniqueSortedBlocks(std::move(task_blocks));
		output->AdoptTaskPrivateBlocks(std::move(task_blocks));
		task_context.SetOutput(std::move(output));
	} catch (...) {
		for (auto &partial_manager : partial_managers) {
			partial_manager->Rollback();
		}
		throw;
	}
}

} // namespace duckdb
