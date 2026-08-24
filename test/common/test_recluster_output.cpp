#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_delete_catchup.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_status.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_drop_ownership_runtime.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/wal_entry.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static ReclusterTaskStartResult StartOutputTask(Connection &con, const string &table_name) {
	ReclusterTaskStartResult result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		auto state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
		REQUIRE(state);
		auto selection = SelectReclusterCandidate(*entry.GetStorage().GetRowGroupCollection(),
		                                          entry.GetStorage().Columns(), *state, {4096, 2, 4, 0.25});
		REQUIRE(selection.status == ReclusterCandidateSelectionStatus::SELECTED);
		REQUIRE(selection.candidate);
		result = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().TryStartTask(
		    entry, *selection.candidate);
	});
	return result;
}

static ReclusterTableStatus GetReclusterStatus(Connection &con, const string &table_name) {
	ReclusterTableStatus result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		result = entry.GetStorage().GetAttached().GetReclusterManager().GetTableStatus(entry);
	});
	return result;
}

static void PrepareOutputTask(ReclusterTaskStartResult &start) {
	ReclusterOutputWriter writer(*start.task);
	writer.Write();
	REQUIRE(start.task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	ReclusterDeleteCatchup catchup(*start.task);
	catchup.Run();
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);
}

static duckdb::vector<duckdb::shared_ptr<RowGroupColumnDropOwnership>>
CaptureLayoutDropOwnership(const duckdb::shared_ptr<const RowGroupLayout> &layout, RowGroupRange range) {
	duckdb::vector<duckdb::shared_ptr<RowGroupColumnDropOwnership>> result;
	reference_set_t<RowGroupColumnDropOwnership> visited;
	LayoutRowGroupCursor cursor(RowGroupCollectionSnapshot(layout), range);
	LayoutRowGroupEntry current;
	while (cursor.Next(current)) {
		for (idx_t column_index = 0; column_index < current.row_group->GetColumnCount(); column_index++) {
			auto tree = CaptureColumnDropOwnershipRuntimeTree(current.row_group->GetRawColumnData(column_index));
			for (auto &node : tree.nodes) {
				auto ownership = node.get().GetDropOwnershipToken();
				REQUIRE(ownership);
				if (visited.insert(reference<RowGroupColumnDropOwnership>(*ownership)).second) {
					result.push_back(std::move(ownership));
				}
			}
		}
	}
	return result;
}

static void RemoveOutputTask(Connection &con, ReclusterTaskStartResult &start, const string &table_name) {
	duckdb::shared_ptr<TableReclusterState> state;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
		state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
	});
	start.task->RequestCancel();
	REQUIRE(start.task->TryEnterCancelling());
	start.task->GetTaskContext().CloseSnapshot();
	REQUIRE(start.task->TryDetach());
	state->RemoveTask(start.task->GetTaskId());
	start.task.reset();
}

class OutputTaskCleanupGuard {
public:
	OutputTaskCleanupGuard(Connection &con_p, ReclusterTaskStartResult &start_p, string table_name_p)
	    : con(con_p), start(start_p), table_name(std::move(table_name_p)) {
	}

	~OutputTaskCleanupGuard() {
		if (!start.task) {
			return;
		}
		try {
			duckdb::shared_ptr<TableReclusterState> state;
			con.context->RunFunctionInTransaction([&]() {
				auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier(table_name)));
				state = entry.GetStorage().GetDataTableInfo()->GetReclusterState();
			});
			start.task->RequestCancel();
			start.task->TryEnterCancelling();
			start.task->GetTaskContext().CloseSnapshot();
			start.task->TryDetach();
			if (state) {
				state->RemoveTask(start.task->GetTaskId());
			}
			start.task.reset();
		} catch (...) {
		}
	}

private:
	Connection &con;
	ReclusterTaskStartResult &start;
	string table_name;
};

static void CheckCollectionRows(Connection &con, RowGroupCollection &collection, MaterializedQueryResult &expected) {
	auto types = collection.GetTypes();
	duckdb::vector<StorageIndex> column_ids;
	for (idx_t column_index = 0; column_index < types.size(); column_index++) {
		column_ids.emplace_back(column_index);
	}

	TableScanState scan_state;
	scan_state.Initialize(column_ids, con.context.get());
	collection.InitializeScan(QueryContext(*con.context), scan_state.table_state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(*con.context, types);
	idx_t output_row = 0;
	while (true) {
		chunk.Reset();
		if (!scan_state.table_state.Scan(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
			break;
		}
		for (idx_t row_index = 0; row_index < chunk.size(); row_index++) {
			for (idx_t column_index = 0; column_index < chunk.ColumnCount(); column_index++) {
				INFO("output row " << output_row << ", column " << column_index);
				REQUIRE(Value::NotDistinctFrom(chunk.GetValue(column_index, row_index),
				                               expected.GetValue(column_index, output_row)));
			}
			output_row++;
		}
	}
	REQUIRE(output_row == expected.RowCount());
}

static ReplacementManifest ReadOutputManifest(ReclusterOutput &output, MetadataManager &metadata_manager) {
	MetadataReader reader(metadata_manager, output.GetManifestPointer());
	return ReplacementManifest::Read(reader);
}

static duckdb::shared_ptr<RowGroupCollection> CreateLazyReplacementCollection(DataTable &storage,
                                                                              ReplacementManifest &manifest) {
	idx_t row_count = 0;
	for (auto &pointer : manifest.replacement_groups) {
		row_count += pointer.tuple_count;
	}
	auto result =
	    make_shared_ptr<RowGroupCollection>(storage.GetDataTableInfo(), storage.GetTableIOManager(), storage.GetTypes(),
	                                        NumericCast<idx_t>(manifest.header.input_range.start), row_count);
	result->InitializeEmpty();
	for (auto &pointer : manifest.replacement_groups) {
		auto row_start = NumericCast<idx_t>(pointer.row_start);
		auto row_group = make_shared_ptr<RowGroup>(*result, std::move(pointer));
		result->GetRowGroups()->AppendSegment(std::move(row_group), row_start);
	}
	result->Verify();
	return result;
}

static duckdb::vector<block_id_t> UniqueBlocks(duckdb::vector<block_id_t> blocks) {
	std::sort(blocks.begin(), blocks.end());
	blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
	return blocks;
}

class ReclusterCatchupMetadataWriterForTest : public MetadataWriter {
public:
	explicit ReclusterCatchupMetadataWriterForTest(TaskPrivateMetadataBlockOwner &owner_p)
	    : MetadataWriter(owner_p.GetManager()), owner(owner_p) {
	}

protected:
	MetadataHandle NextHandle() override {
		return owner.AllocateHandle();
	}

private:
	TaskPrivateMetadataBlockOwner &owner;
};

TEST_CASE("Recluster output writes sorted task-private row groups", "[storage][recluster_output]") {
	auto path = TestCreatePath("recluster_output.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS output_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE output_db"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE tbl(k1 INTEGER, k2 VARCHAR, payload BIGINT, nested STRUCT(a INTEGER, b VARCHAR[]))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT CASE WHEN i % 23 = 0 THEN NULL ELSE (i * 37) % 4096 END, "
	                          "CASE WHEN i % 29 = 0 AND i % 23 <> 0 THEN NULL "
	                          "ELSE lpad(i::VARCHAR, 4, '0') END, i * 101, "
	                          "struct_pack(a := i::INTEGER, b := [i::VARCHAR, NULL::VARCHAR]) "
	                          "FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k1, k2)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_db"));
	auto expected_result = con.Query("SELECT * FROM tbl ORDER BY k1 ASC NULLS LAST, k2 ASC NULLS LAST");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	auto &expected = expected_result->Cast<MaterializedQueryResult>();

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	auto &storage = *start.task->GetTaskContext().GetStorage();
	auto &block_manager = storage.GetTableIOManager().GetBlockManagerForRowData();
	auto &metadata_manager = block_manager.GetMetadataManager();
	auto first_private_block = block_manager.PeekFreeBlockId();
	auto initial_status = GetReclusterStatus(con, "tbl");
	REQUIRE(initial_status.active_prepare_tasks == 1);
	REQUIRE(initial_status.pending_finalize_tasks == 0);
	REQUIRE(initial_status.prepared_bytes == 0);

	ReclusterOutputWriter writer(*start.task);
	try {
		writer.Write();
	} catch (...) {
		RemoveOutputTask(con, start, "tbl");
		throw;
	}
	auto &task_context = start.task->GetTaskContext();
	REQUIRE(task_context.HasOutput());
	REQUIRE(!task_context.HasActiveSnapshot());
	auto output_status = GetReclusterStatus(con, "tbl");
	REQUIRE(output_status.active_prepare_tasks == 1);
	REQUIRE(output_status.pending_finalize_tasks == 0);
	REQUIRE(output_status.prepared_bytes > 0);
	auto &output = task_context.GetOutput();
	REQUIRE(output.GetSortOrderId() == task_context.GetSortDefinition().sort_order_id);
	REQUIRE(output.GetRunId() != INVALID_SORT_RUN_ID);
	REQUIRE(output.GetRowCount() == 4096);
	REQUIRE(output.GetCollection()->GetTotalRows() == 4096);
	REQUIRE(output.GetCollection()->GetNextRowId() == 4096);
	REQUIRE(output.GetPersistentData().row_group_data.size() == 2);
	REQUIRE(output.GetBlockIds().size() >= 8);
	REQUIRE(std::find(output.GetBlockIds().begin(), output.GetBlockIds().end(), first_private_block) !=
	        output.GetBlockIds().end());
	REQUIRE(std::find(output.GetBlockIds().begin(), output.GetBlockIds().end(), block_manager.PeekFreeBlockId()) ==
	        output.GetBlockIds().end());

	auto output_groups = output.GetRowGroups();
	REQUIRE(output_groups.size() == 2);
	for (auto &row_group : output_groups) {
		REQUIRE(row_group->IsPersistent());
		REQUIRE(row_group->IsSealed());
		REQUIRE(row_group->GetSortMetadata() == RowGroupSortMetadata {output.GetSortOrderId(), output.GetRunId()});
		REQUIRE(!row_group->GetOrCreateVersionInfo().HasUncommittedChanges());
	}
	auto output_tree = output.GetCollection()->GetRowGroups();
	auto first_group = output_tree->GetRootSegment();
	REQUIRE(first_group);
	REQUIRE(first_group->GetRowStart() == 0);
	REQUIRE(first_group->GetNode().count == 2048);
	auto second_group = output_tree->GetNextSegment(*first_group);
	REQUIRE(second_group);
	REQUIRE(second_group->GetRowStart() == 2048);
	REQUIRE(second_group->GetNode().count == 2048);
	REQUIRE(!output_tree->GetNextSegment(*second_group));
	CheckCollectionRows(con, *output.GetCollection(), expected);

	auto &manifest = output.GetManifest();
	REQUIRE(manifest.header.task_id == start.task->GetTaskId());
	REQUIRE(manifest.header.table_id == task_context.GetTableId());
	REQUIRE(manifest.header.prepared_layout_version == task_context.GetCandidate().layout_version);
	REQUIRE(manifest.header.sort_order_id == output.GetSortOrderId());
	REQUIRE(manifest.header.run_id == output.GetRunId());
	REQUIRE(manifest.header.input_range.start == 0);
	REQUIRE(manifest.header.input_range.end == 4096);
	REQUIRE(manifest.header.last_applied_delete_sequence == 0);
	REQUIRE(manifest.header.manifest_revision == 1);
	REQUIRE(manifest.sort_columns == task_context.GetSortDefinition().columns);
	REQUIRE(manifest.old_groups == task_context.GetCandidate().expected_row_groups);
	REQUIRE(manifest.replacement_groups.size() == 2);
	REQUIRE(manifest.replacement_groups[0].row_start == 0);
	REQUIRE(manifest.replacement_groups[0].tuple_count == 2048);
	REQUIRE(manifest.replacement_groups[1].row_start == 2048);
	REQUIRE(manifest.replacement_groups[1].tuple_count == 2048);
	REQUIRE(manifest.replacement_groups[0].data_pointers.size() == 4);
	REQUIRE(manifest.replacement_groups[1].data_pointers.size() == 4);
	REQUIRE(manifest.all_referenced_blocks.size() < output.GetBlockIds().size());
	REQUIRE(!std::binary_search(manifest.all_referenced_blocks.begin(), manifest.all_referenced_blocks.end(),
	                            output.GetManifestPointer().GetBlockId()));
	for (auto block_id : manifest.all_referenced_blocks) {
		REQUIRE(std::binary_search(output.GetBlockIds().begin(), output.GetBlockIds().end(), block_id));
	}

	auto loaded_manifest = ReadOutputManifest(output, metadata_manager);
	REQUIRE(loaded_manifest.header.task_id == manifest.header.task_id);
	REQUIRE(loaded_manifest.payload_size == manifest.payload_size);
	REQUIRE(loaded_manifest.checksum == manifest.checksum);
	REQUIRE(loaded_manifest.all_referenced_blocks == manifest.all_referenced_blocks);

	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_db"));
	auto data_blocks = UniqueBlocks(output.GetPersistentData().GetBlockIds());
	for (auto block_id : output.GetBlockIds()) {
		if (std::binary_search(data_blocks.begin(), data_blocks.end(), block_id)) {
			continue;
		}
		for (auto &info : metadata_manager.GetMetadataInfo()) {
			REQUIRE(info.block_id != block_id);
		}
	}
	auto checkpoint_manifest = ReadOutputManifest(output, metadata_manager);
	REQUIRE(checkpoint_manifest.checksum == manifest.checksum);
	auto lazy_collection = CreateLazyReplacementCollection(storage, checkpoint_manifest);
	REQUIRE(lazy_collection->GetRowGroupCount() == 2);
	for (idx_t row_group_index = 0; row_group_index < lazy_collection->GetRowGroupCount(); row_group_index++) {
		auto row_group = lazy_collection->GetRowGroup(NumericCast<int64_t>(row_group_index));
		REQUIRE(row_group);
		REQUIRE(row_group->GetColumnStartPointers().size() == 4);
		for (auto &pointer : row_group->GetColumnStartPointers()) {
			REQUIRE(pointer.IsValid());
		}
	}
	CheckCollectionRows(con, *lazy_collection, expected);

	auto main_result = con.Query("SELECT count(*), count(DISTINCT payload) FROM tbl");
	REQUIRE_NO_FAIL(*main_result);
	REQUIRE(CHECK_COLUMN(main_result, 0, {4096}));
	REQUIRE(CHECK_COLUMN(main_result, 1, {4096}));
	REQUIRE(start.task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	ReclusterDeleteCatchup catchup(*start.task);
	catchup.Run();
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);
	auto checkpoint_lock = storage.GetAttached().GetReclusterManager().GetExclusiveLayoutPublishLock();
	auto pending_status = GetReclusterStatus(con, "tbl");
	REQUIRE(pending_status.active_prepare_tasks == 0);
	REQUIRE(pending_status.pending_finalize_tasks == 1);
	REQUIRE(pending_status.blocked_reason == "CHECKPOINT_IN_PROGRESS");
	checkpoint_lock.reset();
	lazy_collection.reset();
	output_groups.clear();
	output_tree.reset();
	RemoveOutputTask(con, start, "tbl");
	REQUIRE(block_manager.PeekFreeBlockId() == first_private_block);
	DeleteDatabase(path);
}

TEST_CASE("Recluster output supports an empty replacement", "[storage][recluster_output]") {
	auto path = TestCreatePath("recluster_output_empty.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS output_empty_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT output_empty_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl"));

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	ReclusterOutputWriter writer(*start.task);
	try {
		writer.Write();
	} catch (...) {
		RemoveOutputTask(con, start, "tbl");
		throw;
	}
	auto &output = start.task->GetTaskContext().GetOutput();
	REQUIRE(output.GetRowCount() == 0);
	REQUIRE(output.GetCollection()->GetRowGroupCount() == 0);
	REQUIRE(output.GetPersistentData().row_group_data.empty());
	REQUIRE(!output.GetBlockIds().empty());
	REQUIRE(output.GetRowGroups().empty());
	REQUIRE(!start.task->GetTaskContext().HasActiveSnapshot());
	REQUIRE(output.GetManifest().replacement_groups.empty());
	REQUIRE(output.GetManifest().all_referenced_blocks.empty());
	REQUIRE(std::binary_search(output.GetBlockIds().begin(), output.GetBlockIds().end(),
	                           output.GetManifestPointer().GetBlockId()));
	auto &storage = *start.task->GetTaskContext().GetStorage();
	auto &metadata_manager = storage.GetTableIOManager().GetBlockManagerForRowData().GetMetadataManager();
	auto manifest = ReadOutputManifest(output, metadata_manager);
	REQUIRE(manifest.replacement_groups.empty());
	REQUIRE(manifest.all_referenced_blocks.empty());
	REQUIRE(manifest.old_groups == start.task->GetTaskContext().GetCandidate().expected_row_groups);

	RemoveOutputTask(con, start, "tbl");
	DeleteDatabase(path);
}

TEST_CASE("Recluster DELETE catch-up persists resolved journal prefixes", "[storage][recluster_delete_catchup]") {
	auto path = TestCreatePath("recluster_delete_catchup.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS catchup_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE catchup_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k INTEGER, payload BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT ((i * 37) % 4096)::INTEGER, i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT catchup_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT catchup_db"));

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	auto &storage = *start.task->GetTaskContext().GetStorage();
	auto &block_manager = storage.GetTableIOManager().GetBlockManagerForRowData();
	auto first_private_block = block_manager.PeekFreeBlockId();
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload IN (7, 2050, 4095)"));
	REQUIRE(start.task->GetLatestDeleteSequence() == 1);
	auto aborted_slot = start.task->TryReserveDeleteSlot({11});
	REQUIRE(aborted_slot);
	REQUIRE(start.task->ResolveDeleteSlot(*aborted_slot, DeleteSlotState::ABORTED));
	auto reserved_slot = start.task->TryReserveDeleteSlot({13});
	REQUIRE(reserved_slot);
	REQUIRE(start.task->GetLatestDeleteSequence() == 3);

	ReclusterOutputWriter writer(*start.task);
	writer.Write();
	REQUIRE(start.task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	ReclusterDeleteCatchup catchup(*start.task);
	auto first_result = catchup.Run(1, start.task->GetDeleteJournalLimits().max_rowids);
	REQUIRE(first_result.applied_through == 1);
	REQUIRE(first_result.resolved_slot_count == 1);
	REQUIRE(first_result.mapped_rowid_count == 3);
	REQUIRE(first_result.deleted_row_count == 3);
	REQUIRE(first_result.limit_exceeded);
	REQUIRE(start.task->GetState() == RangeTaskState::CATCHING_UP_DELETES);

	auto aborted_result = catchup.Run(1, start.task->GetDeleteJournalLimits().max_rowids);
	REQUIRE(aborted_result.applied_through == 2);
	REQUIRE(aborted_result.resolved_slot_count == 1);
	REQUIRE(aborted_result.mapped_rowid_count == 0);
	REQUIRE(aborted_result.deleted_row_count == 0);
	REQUIRE(aborted_result.blocked_by_reserved);
	REQUIRE(!aborted_result.limit_exceeded);
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);

	auto &task_context = start.task->GetTaskContext();
	auto &output = task_context.GetOutput();
	REQUIRE(output.GetManifest().header.last_applied_delete_sequence == 2);
	REQUIRE(output.GetManifest().header.manifest_revision == 3);
	idx_t delete_pointer_count = 0;
	duckdb::vector<block_id_t> first_delete_blocks;
	for (auto &row_group : output.GetManifest().replacement_groups) {
		delete_pointer_count += row_group.deletes_pointers.size();
		for (auto &pointer : row_group.deletes_pointers) {
			first_delete_blocks.push_back(pointer.GetBlockId());
			REQUIRE(std::binary_search(output.GetManifest().all_referenced_blocks.begin(),
			                           output.GetManifest().all_referenced_blocks.end(), pointer.GetBlockId()));
		}
	}
	REQUIRE(delete_pointer_count > 0);
	first_delete_blocks = UniqueBlocks(std::move(first_delete_blocks));
	auto expected_result = con.Query("SELECT * FROM tbl ORDER BY k ASC NULLS LAST");
	REQUIRE(expected_result);
	REQUIRE(!expected_result->HasError());
	CheckCollectionRows(con, *output.GetCollection(), expected_result->Cast<MaterializedQueryResult>());

	REQUIRE(start.task->ResolveDeleteSlot(*reserved_slot, DeleteSlotState::ABORTED));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload IN (19, 3072)"));
	REQUIRE(start.task->GetLatestDeleteSequence() == 4);
	auto previous_manifest_pointer = output.GetManifestPointer();
	REQUIRE(start.task->TryAdvance(RangeTaskState::PREPARED, RangeTaskState::FINALIZING));
	REQUIRE(start.task->TryAdvance(RangeTaskState::FINALIZING, RangeTaskState::CATCHING_UP_DELETES));
	ReclusterDeleteCatchup second_catchup(*start.task);
	auto second_result = second_catchup.Run();
	REQUIRE(second_result.applied_through == 4);
	REQUIRE(second_result.resolved_slot_count == 2);
	REQUIRE(second_result.mapped_rowid_count == 2);
	REQUIRE(second_result.deleted_row_count == 2);
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);
	REQUIRE(output.GetManifest().header.last_applied_delete_sequence == 4);
	REQUIRE(output.GetManifest().header.manifest_revision == 4);
	REQUIRE(!(output.GetManifestPointer() == previous_manifest_pointer));
	REQUIRE(!std::binary_search(output.GetBlockIds().begin(), output.GetBlockIds().end(),
	                            previous_manifest_pointer.GetBlockId()));
	for (auto block_id : first_delete_blocks) {
		REQUIRE(!std::binary_search(output.GetBlockIds().begin(), output.GetBlockIds().end(), block_id));
	}

	auto &metadata_manager = block_manager.GetMetadataManager();
	auto loaded_manifest = ReadOutputManifest(output, metadata_manager);
	REQUIRE(loaded_manifest.header.last_applied_delete_sequence == 4);
	REQUIRE(loaded_manifest.header.manifest_revision == 4);
	REQUIRE(loaded_manifest.checksum == output.GetManifest().checksum);
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT catchup_db"));
	auto lazy_collection = CreateLazyReplacementCollection(storage, loaded_manifest);
	auto final_expected_result = con.Query("SELECT * FROM tbl ORDER BY k ASC NULLS LAST");
	REQUIRE(final_expected_result);
	REQUIRE(!final_expected_result->HasError());
	CheckCollectionRows(con, *output.GetCollection(), final_expected_result->Cast<MaterializedQueryResult>());
	CheckCollectionRows(con, *lazy_collection, final_expected_result->Cast<MaterializedQueryResult>());

	lazy_collection.reset();
	RemoveOutputTask(con, start, "tbl");
	REQUIRE(block_manager.PeekFreeBlockId() == first_private_block);
	DeleteDatabase(path);
}

TEST_CASE("Recluster DELETE catch-up skips rows absent from an empty replacement",
          "[storage][recluster_delete_catchup]") {
	auto path = TestCreatePath("recluster_delete_catchup_empty.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS catchup_empty_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE catchup_empty_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT i::INTEGER FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT catchup_empty_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT catchup_empty_db"));
	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl"));

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	ReclusterOutputWriter writer(*start.task);
	writer.Write();
	auto committed_slot = start.task->TryReserveDeleteSlot({start.task->GetRange().start});
	REQUIRE(committed_slot);
	REQUIRE(start.task->ResolveDeleteSlot(*committed_slot, DeleteSlotState::COMMITTED));
	REQUIRE(start.task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	ReclusterDeleteCatchup catchup(*start.task);
	auto result = catchup.Run();
	REQUIRE(result.applied_through == 1);
	REQUIRE(result.mapped_rowid_count == 0);
	REQUIRE(result.deleted_row_count == 0);
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);

	auto &output = start.task->GetTaskContext().GetOutput();
	REQUIRE(output.GetManifest().header.last_applied_delete_sequence == 1);
	REQUIRE(output.GetManifest().header.manifest_revision == 2);
	REQUIRE(output.GetManifest().replacement_groups.empty());
	REQUIRE(output.GetManifest().all_referenced_blocks.empty());
	auto &storage = *start.task->GetTaskContext().GetStorage();
	auto &metadata_manager = storage.GetTableIOManager().GetBlockManagerForRowData().GetMetadataManager();
	auto loaded_manifest = ReadOutputManifest(output, metadata_manager);
	REQUIRE(loaded_manifest.header.last_applied_delete_sequence == 1);
	REQUIRE(loaded_manifest.header.manifest_revision == 2);
	REQUIRE(loaded_manifest.replacement_groups.empty());
	REQUIRE(loaded_manifest.all_referenced_blocks.empty());

	RemoveOutputTask(con, start, "tbl");
	DeleteDatabase(path);
}

TEST_CASE("Recluster committed DELETE metadata spans task-private blocks", "[storage][recluster_delete_catchup]") {
	auto path = TestCreatePath("recluster_delete_metadata_blocks.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(
	    con.Query("ATTACH '" + path + "' AS delete_blocks_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE delete_blocks_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT delete_blocks_db"));

	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto &block_manager = entry.GetStorage().GetTableIOManager().GetBlockManagerForRowData();
		auto &metadata_manager = block_manager.GetMetadataManager();
		RowVersionManager version_manager(block_manager.GetBufferManager());
		auto vector_count = metadata_manager.GetMetadataBlockSize() / ValidityMask::STANDARD_MASK_SIZE + 2;
		duckdb::vector<row_t> row_offsets(STANDARD_VECTOR_SIZE / 2);
		for (idx_t row_index = 0; row_index < row_offsets.size(); row_index++) {
			row_offsets[row_index] = NumericCast<row_t>(row_index);
		}
		for (idx_t vector_index = 0; vector_index < vector_count; vector_index++) {
			REQUIRE(version_manager.DeleteCommittedRows(vector_index, row_offsets.data(), row_offsets.size()) ==
			        row_offsets.size());
		}
		row_t invalid_offset = NumericCast<row_t>(STANDARD_VECTOR_SIZE);
		REQUIRE_THROWS(version_manager.DeleteCommittedRows(0, &invalid_offset, 1));
		REQUIRE(version_manager.HasUnserializedChanges());
		REQUIRE(!version_manager.HasUncommittedChanges());

		auto owner = metadata_manager.CreateTaskPrivateBlockOwner();
		duckdb::vector<MetaBlockPointer> pointers;
		{
			ReclusterCatchupMetadataWriterForTest metadata_writer(*owner);
			pointers = version_manager.Checkpoint(metadata_writer, 0);
			metadata_writer.Flush();
		}
		owner->Flush();
		block_manager.FileSync();
		REQUIRE(pointers.size() > 1);
		REQUIRE(!version_manager.HasUnserializedChanges());

		auto loaded = RowVersionManager::Deserialize(pointers[0], metadata_manager);
		REQUIRE(loaded);
		REQUIRE(loaded->GetStoragePointers() == pointers);
		duckdb::vector<idx_t> deleted_offsets;
		duckdb::vector<idx_t> live_offsets;
		for (idx_t vector_index = 0; vector_index < vector_count; vector_index++) {
			deleted_offsets.push_back(vector_index * STANDARD_VECTOR_SIZE);
			live_offsets.push_back(vector_index * STANDARD_VECTOR_SIZE + STANDARD_VECTOR_SIZE / 2);
		}
		SelectionVector visible(vector_count);
		REQUIRE(loaded->GetVisibleRows(TransactionData(1, 1), deleted_offsets.data(), deleted_offsets.size(),
		                               visible) == 0);
		REQUIRE(loaded->GetVisibleRows(TransactionData(1, 1), live_offsets.data(), live_offsets.size(), visible) ==
		        vector_count);
		loaded.reset();
		owner->Abort();
	});

	DeleteDatabase(path);
}

TEST_CASE("Recluster finalize atomically publishes a replacement layout", "[storage][recluster_finalize]") {
	auto path = TestCreatePath("recluster_finalize.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS finalize_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE finalize_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k INTEGER, payload BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT ((i * 37) % 4096)::INTEGER, i FROM range(4096) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));

	auto start = StartOutputTask(con, "tbl");
	REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
	REQUIRE(start.task);
	OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
	PrepareOutputTask(start);
	auto storage = start.task->GetTaskContext().GetStorage();
	auto collection = storage->GetRowGroupCollection();
	auto state = storage->GetDataTableInfo()->GetReclusterState();
	auto old_layout = collection->GetCurrentLayout();
	auto old_layout_version = storage->GetDataTableInfo()->GetSortStorage().current_layout_version.load();
	REQUIRE(old_layout);
	REQUIRE(old_layout->layout_version == old_layout_version);
	auto old_ownership = CaptureLayoutDropOwnership(old_layout, start.task->GetRange());
	REQUIRE(!old_ownership.empty());
	auto runtime_wal_blocks = start.task->GetTaskContext().GetOutput().GetBlockIds();
	REQUIRE(!runtime_wal_blocks.empty());
	auto &manager = storage->GetAttached().GetReclusterManager();
	auto &retirement = manager.GetRetirementRegistry();
	auto &wal_retention = manager.GetWALBlockRetention();
	auto &block_manager = storage->GetAttached().GetStorageManager().GetBlockManager();
	REQUIRE(retirement.Count() == 0);
	REQUIRE(retirement.GetRetiredBytes(*storage->GetDataTableInfo()) == 0);
	REQUIRE(wal_retention.Count() == 0);
	auto unresolved_slot = start.task->TryReserveDeleteSlot({23});
	REQUIRE(unresolved_slot);
	ReclusterTaskFinalizeStatus status = ReclusterTaskFinalizeStatus::STALE_TASK;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		status = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().FinalizeTask(entry, start.task);
	});
	REQUIRE(status == ReclusterTaskFinalizeStatus::RETRY);
	REQUIRE(start.task->GetState() == RangeTaskState::PREPARED);
	REQUIRE(state->OwnsTask(start.task));
	REQUIRE(collection->GetCurrentLayout().get() == old_layout.get());
	REQUIRE(retirement.Count() == 0);
	REQUIRE(wal_retention.Count() == 0);
	for (auto &ownership : old_ownership) {
		REQUIRE(ownership->GetState() == RowGroupColumnDropOwnership::State::LIVE);
	}
	REQUIRE(start.task->ResolveDeleteSlot(*unresolved_slot, DeleteSlotState::ABORTED));

	Connection old_reader(db);
	REQUIRE_NO_FAIL(old_reader.Query("USE finalize_db"));
	old_reader.BeginTransaction();
	auto old_count = old_reader.Query("SELECT count(*) FROM tbl");
	REQUIRE(old_count);
	REQUIRE(CHECK_COLUMN(old_count, 0, {4096}));

	REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload = 19"));
	REQUIRE(start.task->GetLatestDeleteSequence() == 2);
	REQUIRE(start.task->GetTaskContext().GetOutput().GetManifest().header.last_applied_delete_sequence == 0);

	status = ReclusterTaskFinalizeStatus::STALE_TASK;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		status = entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().FinalizeTask(entry, start.task);
	});
	REQUIRE(status == ReclusterTaskFinalizeStatus::PUBLISHED);
	REQUIRE(start.task->GetState() == RangeTaskState::PUBLISHED);
	REQUIRE(!state->GetTask(start.task->GetTaskId()));
	REQUIRE(retirement.Count() == 1);
	REQUIRE(retirement.GetRetiredBytes(*storage->GetDataTableInfo()) > 0);
	REQUIRE(wal_retention.Count() == 1);
	for (auto block_id : runtime_wal_blocks) {
		REQUIRE(block_manager.IsBlockReserved(block_id));
	}
	for (auto &ownership : old_ownership) {
		REQUIRE(ownership->GetState() == RowGroupColumnDropOwnership::State::LIVE);
	}

	auto published_layout = collection->GetCurrentLayout();
	REQUIRE(published_layout);
	REQUIRE(published_layout.get() != old_layout.get());
	REQUIRE(published_layout->layout_version == old_layout_version + 1);
	REQUIRE(published_layout->patches.size() == 1);
	REQUIRE(published_layout->patches[0]->task_id == start.task->GetTaskId());
	REQUIRE(storage->GetDataTableInfo()->GetSortStorage().current_layout_version.load() == old_layout_version + 1);
	duckdb::weak_ptr<const RowGroupLayout> old_layout_reference(old_layout);
	old_layout.reset();

	{
		auto abandoned_checkpoint = retirement.PrepareCheckpoint();
		REQUIRE(!abandoned_checkpoint.Empty());
	}
	REQUIRE(retirement.Count() == 1);
	for (auto &ownership : old_ownership) {
		REQUIRE(ownership->GetState() == RowGroupColumnDropOwnership::State::LIVE);
	}

	REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));
	REQUIRE(retirement.Count() == 1);
	REQUIRE(retirement.GetRetiredBytes(*storage->GetDataTableInfo()) > 0);
	REQUIRE(wal_retention.Count() == 0);
	REQUIRE(!old_layout_reference.expired());
	for (auto block_id : runtime_wal_blocks) {
		REQUIRE(!block_manager.IsBlockReserved(block_id));
	}
	for (auto &ownership : old_ownership) {
		REQUIRE(ownership->GetState() == RowGroupColumnDropOwnership::State::DROPPED);
	}

	auto old_rows = old_reader.Query("SELECT count(*), count(*) FILTER (WHERE payload = 19) FROM tbl");
	REQUIRE(old_rows);
	REQUIRE(CHECK_COLUMN(old_rows, 0, {4096}));
	REQUIRE(CHECK_COLUMN(old_rows, 1, {1}));
	auto current_rows = con.Query("SELECT count(*), count(*) FILTER (WHERE payload = 19) FROM tbl");
	REQUIRE(current_rows);
	REQUIRE(CHECK_COLUMN(current_rows, 0, {4095}));
	REQUIRE(CHECK_COLUMN(current_rows, 1, {0}));
	old_reader.Rollback();

	start.task.reset();
	REQUIRE_NO_FAIL(con.Query("SELECT 1"));
	retirement.Cleanup();
	REQUIRE(old_layout_reference.expired());
	REQUIRE(retirement.Count() == 0);
	REQUIRE(retirement.GetRetiredBytes(*storage->GetDataTableInfo()) == 0);
	DeleteDatabase(path);
}

TEST_CASE("Recluster finalize restores the old layout after commit failure", "[storage][recluster_finalize]") {
	auto path = TestCreatePath("recluster_finalize_failure.db");
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS finalize_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE finalize_db"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k INTEGER, payload BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT ((i * 37) % 4096)::INTEGER, i FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));

		auto start = StartOutputTask(con, "tbl");
		REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
		REQUIRE(start.task);
		OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
		PrepareOutputTask(start);
		auto storage = start.task->GetTaskContext().GetStorage();
		auto collection = storage->GetRowGroupCollection();
		auto state = storage->GetDataTableInfo()->GetReclusterState();
		auto old_layout = collection->GetCurrentLayout();
		auto old_layout_version = storage->GetDataTableInfo()->GetSortStorage().current_layout_version.load();

		REQUIRE_NO_FAIL(con.Query("SET debug_force_commit_failure=true"));
		string error;
		try {
			con.context->RunFunctionInTransaction([&]() {
				auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
				entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().FinalizeTask(entry, start.task);
			});
		} catch (std::exception &ex) {
			error = ex.what();
		}
		REQUIRE(error.find("Forced commit failure") != string::npos);
		REQUIRE_NO_FAIL(con.Query("SET debug_force_commit_failure=false"));
		REQUIRE(start.task->GetState() == RangeTaskState::FAILED);
		REQUIRE(!state->GetTask(start.task->GetTaskId()));
		REQUIRE(storage->GetAttached().GetReclusterManager().GetRetirementRegistry().Count() == 0);
		REQUIRE(storage->GetAttached().GetReclusterManager().GetWALBlockRetention().Count() == 0);
		REQUIRE(collection->GetCurrentLayout().get() == old_layout.get());
		REQUIRE(storage->GetDataTableInfo()->GetSortStorage().current_layout_version.load() == old_layout_version);
		auto rows = con.Query("SELECT count(*), sum(payload) FROM tbl");
		REQUIRE(rows);
		REQUIRE(CHECK_COLUMN(rows, 0, {4096}));
		REQUIRE(CHECK_COLUMN(rows, 1, {8386560}));
		start.task.reset();
	}

	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		auto rows = con.Query("SELECT count(*), sum(payload) FROM tbl");
		REQUIRE(rows);
		REQUIRE(CHECK_COLUMN(rows, 0, {4096}));
		REQUIRE(CHECK_COLUMN(rows, 1, {8386560}));
	}
	DeleteDatabase(path);
}

struct CommittedReclusterWAL {
	WALReclusterEntry header;
	duckdb::vector<block_id_t> retained_blocks;
	duckdb::vector<block_id_t> replacement_blocks;
	idx_t previous_flush_end = 0;
};

static AttachedDatabase &GetReclusterDatabase(DuckDB &db, Connection &con) {
	auto database_name = DatabaseManager::GetDefaultDatabase(*con.context);
	auto attached = db.instance->GetDatabaseManager().GetDatabase(database_name);
	REQUIRE(attached);
	return *attached;
}

static CommittedReclusterWAL CreateCommittedReclusterWAL(const string &path, bool add_final_delete) {
	DeleteDatabase(path);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(
		    con.Query("ATTACH '" + path + "' AS finalize_db (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v2.0.0')"));
		REQUIRE_NO_FAIL(con.Query("USE finalize_db"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(k INTEGER, payload BIGINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tbl SELECT ((i * 37) % 4096)::INTEGER, i FROM range(4096) t(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE tbl SET SORTED BY (k)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT finalize_db"));

		auto start = StartOutputTask(con, "tbl");
		REQUIRE(start.status == ReclusterTaskStartStatus::STARTED);
		REQUIRE(start.task);
		OutputTaskCleanupGuard cleanup_guard(con, start, "tbl");
		PrepareOutputTask(start);

		CommittedReclusterWAL result;
		if (add_final_delete) {
			REQUIRE_NO_FAIL(con.Query("DELETE FROM tbl WHERE payload = 19"));
		}
		result.previous_flush_end = GetReclusterDatabase(db, con).GetStorageManager().GetWALSize();

		ReclusterTaskFinalizeStatus status = ReclusterTaskFinalizeStatus::STALE_TASK;
		con.context->RunFunctionInTransaction([&]() {
			auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
			status =
			    entry.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager().FinalizeTask(entry, start.task);
		});
		REQUIRE(status == ReclusterTaskFinalizeStatus::PUBLISHED);
		auto &output = start.task->GetTaskContext().GetOutput();
		auto &manifest = output.GetManifest();
		result.header.table_id = manifest.header.table_id;
		result.header.task_id = manifest.header.task_id;
		result.header.expected_layout_version = manifest.header.prepared_layout_version;
		result.header.target_layout_version = result.header.expected_layout_version + 1;
		result.header.range_start = manifest.header.input_range.start;
		result.header.range_end = manifest.header.input_range.end;
		result.header.manifest_pointer = output.GetManifestPointer();
		result.header.manifest_size = manifest.payload_size;
		result.header.manifest_checksum = manifest.checksum;
		result.header.journal_resolved_through = manifest.header.last_applied_delete_sequence;
		result.retained_blocks = output.GetBlockIds();
		result.replacement_blocks = manifest.all_referenced_blocks;
		REQUIRE(!result.retained_blocks.empty());
		REQUIRE(!result.replacement_blocks.empty());
		start.task.reset();
		return result;
	}
	return {};
}

static void CheckRecoveredRecluster(Connection &con, bool payload_deleted, layout_version_t expected_version,
                                    idx_t expected_patch_count) {
	auto rows = con.Query("SELECT count(*), sum(payload), count(*) FILTER (WHERE payload = 19) FROM tbl");
	REQUIRE(rows);
	REQUIRE(CHECK_COLUMN(rows, 0, {payload_deleted ? 4095 : 4096}));
	REQUIRE(CHECK_COLUMN(rows, 1, {payload_deleted ? 8386541 : 8386560}));
	REQUIRE(CHECK_COLUMN(rows, 2, {payload_deleted ? 0 : 1}));
	auto expected = con.Query("SELECT k, payload FROM tbl ORDER BY k");
	REQUIRE(expected);
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		auto &storage = entry.GetStorage();
		auto collection = storage.GetRowGroupCollection();
		auto layout = collection->GetCurrentLayout();
		REQUIRE(layout);
		REQUIRE(layout->layout_version == expected_version);
		REQUIRE(layout->patches.size() == expected_patch_count);
		auto &sort_storage = storage.GetDataTableInfo()->GetSortStorage();
		REQUIRE(sort_storage.current_layout_version.load() == expected_version);
		for (auto &patch : layout->patches) {
			REQUIRE(patch->sort_order_id != INVALID_SORT_ORDER_ID);
			REQUIRE(patch->run_id != INVALID_SORT_RUN_ID);
			REQUIRE(sort_storage.next_run_id.load() > patch->run_id);
		}
		if (expected_version > INITIAL_LAYOUT_VERSION) {
			CheckCollectionRows(con, *collection, expected->Cast<MaterializedQueryResult>());
		}
	});
}

static void CheckRecoveredRecluster(const string &path, bool payload_deleted, layout_version_t expected_version,
                                    idx_t expected_patch_count) {
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	CheckRecoveredRecluster(con, payload_deleted, expected_version, expected_patch_count);
}

static idx_t GetReclusterWALFileSize(const string &path) {
	LocalFileSystem fs;
	auto handle = fs.OpenFile(path + ".wal", FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	return handle ? NumericCast<idx_t>(handle->GetFileSize()) : 0;
}

static idx_t TruncateReclusterWALTail(const string &path) {
	LocalFileSystem fs;
	auto handle = fs.OpenFile(path + ".wal", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	REQUIRE(file_size > 0);
	handle->Truncate(NumericCast<int64_t>(file_size - 1));
	handle->Sync();
	return NumericCast<idx_t>(file_size - 1);
}

static idx_t AppendReclusterWAL(const string &path, const WALReclusterEntry &header, bool checkpoint_before,
                                bool commit) {
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
	if (checkpoint_before) {
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}

	auto &storage_manager = GetReclusterDatabase(db, con).GetStorageManager();
	auto previous_flush_end = storage_manager.GetWALSize();
	auto wal_lock = storage_manager.GetWALLock();
	auto wal = storage_manager.GetWAL();
	REQUIRE(wal);
	wal->WriteRecluster(header);
	if (commit) {
		wal->Flush();
	}
	return previous_flush_end;
}

static void CheckReclusterBlocksReserved(AttachedDatabase &db, const CommittedReclusterWAL &wal, bool expected) {
	auto &block_manager = db.GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
	for (auto block_id : wal.retained_blocks) {
		REQUIRE(block_manager.IsBlockReserved(block_id) == expected);
	}
}

enum class ReclusterRecoveryFault : uint8_t { TRUNCATE, SYNC };

struct ReclusterRecoveryFaultState {
	bool truncate_seen = false;
	bool sync_seen = false;
};

class ReclusterRecoveryFaultFileSystem : public LocalFileSystem {
public:
	ReclusterRecoveryFaultFileSystem(string wal_path_p, ReclusterRecoveryFault fault_p,
	                                 ReclusterRecoveryFaultState &state_p)
	    : wal_path(std::move(wal_path_p)), fault(fault_p), state(state_p) {
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		if (!IsTargetWAL(handle)) {
			return LocalFileSystem::Truncate(handle, new_size);
		}
		state.truncate_seen = true;
		if (fault == ReclusterRecoveryFault::TRUNCATE) {
			throw IOException("Injected recluster WAL truncate failure");
		}
		LocalFileSystem::Truncate(handle, new_size);
		wait_for_sync = true;
	}

	void FileSync(FileHandle &handle) override {
		if (IsTargetWAL(handle) && wait_for_sync) {
			state.sync_seen = true;
			wait_for_sync = false;
			if (fault == ReclusterRecoveryFault::SYNC) {
				throw IOException("Injected recluster WAL sync failure");
			}
		}
		LocalFileSystem::FileSync(handle);
	}

private:
	bool IsTargetWAL(FileHandle &handle) const {
		return handle.GetPath() == wal_path || StringUtil::EndsWith(handle.GetPath(), ".wal");
	}

private:
	string wal_path;
	ReclusterRecoveryFault fault;
	ReclusterRecoveryFaultState &state;
	bool wait_for_sync = false;
};

TEST_CASE("Recluster WAL recovery applies a committed replacement", "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_recovery.db");
	auto wal = CreateCommittedReclusterWAL(path, true);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &attached = GetReclusterDatabase(db, con);
		auto &retention = attached.GetReclusterManager().GetWALBlockRetention();
		auto &retirement = attached.GetReclusterManager().GetRetirementRegistry();
		REQUIRE(retention.Count() == 1);
		REQUIRE(retirement.Count() == 1);
		CheckReclusterBlocksReserved(attached, wal, true);
		CheckRecoveredRecluster(con, true, 1, 1);

		REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
		REQUIRE(retention.Count() == 0);
		REQUIRE(retirement.Count() == 0);
		CheckReclusterBlocksReserved(attached, wal, false);
	}
	CheckRecoveredRecluster(path, true, 1, 0);
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery reads protected blocks through MMAP",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_mmap_recovery.db");
	CreateCommittedReclusterWAL(path, true);
	{
		DuckDB db;
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS recovery_db (IO_MODE 'MMAP', MMAP_RESERVE_SIZE '1GB')"));
		REQUIRE_NO_FAIL(con.Query("USE recovery_db"));
		CheckRecoveredRecluster(con, true, 1, 1);
	}
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery ignores a torn maintenance transaction",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_torn_recovery.db");
	auto wal = CreateCommittedReclusterWAL(path, true);
	auto torn_size = TruncateReclusterWALTail(path);
	REQUIRE(torn_size > wal.previous_flush_end);
	{
		DBConfig config;
		config.options.access_mode = AccessMode::READ_ONLY;
		DuckDB db(path, &config);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		auto &attached = GetReclusterDatabase(db, con);
		REQUIRE(attached.GetReclusterManager().GetWALBlockRetention().Count() == 1);
		CheckReclusterBlocksReserved(attached, wal, true);
		CheckRecoveredRecluster(con, true, 0, 0);
		REQUIRE(GetReclusterWALFileSize(path) == torn_size);
	}
	REQUIRE(GetReclusterWALFileSize(path) == torn_size);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &attached = GetReclusterDatabase(db, con);
		REQUIRE(attached.GetReclusterManager().GetWALBlockRetention().Count() == 0);
		CheckReclusterBlocksReserved(attached, wal, false);
		CheckRecoveredRecluster(con, true, 0, 0);
		REQUIRE(GetReclusterWALFileSize(path) == wal.previous_flush_end);
	}
	CheckRecoveredRecluster(path, true, 0, 0);
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery fails if torn tail cleanup is not durable",
          "[storage][recluster_finalize][recluster_wal]") {
	for (auto fault : {ReclusterRecoveryFault::TRUNCATE, ReclusterRecoveryFault::SYNC}) {
		auto suffix = fault == ReclusterRecoveryFault::TRUNCATE ? "truncate" : "sync";
		auto path = TestCreatePath("recluster_finalize_torn_" + string(suffix) + "_failure.db");
		auto wal = CreateCommittedReclusterWAL(path, true);
		auto torn_size = TruncateReclusterWALTail(path);

		ReclusterRecoveryFaultState state;
		DBConfig config;
		duckdb::unique_ptr<FileSystem> local_fs =
		    make_uniq<ReclusterRecoveryFaultFileSystem>(path + ".wal", fault, state);
		config.file_system = make_uniq<VirtualFileSystem>(std::move(local_fs));
		string error;
		try {
			DuckDB reopened(path, &config);
		} catch (std::exception &ex) {
			error = ex.what();
		}
		REQUIRE(error.find("Injected recluster WAL") != string::npos);
		REQUIRE(state.truncate_seen);
		REQUIRE(state.sync_seen == (fault == ReclusterRecoveryFault::SYNC));
		REQUIRE(GetReclusterWALFileSize(path) ==
		        (fault == ReclusterRecoveryFault::TRUNCATE ? torn_size : wal.previous_flush_end));

		{
			DuckDB db(path);
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
			REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
			CheckRecoveredRecluster(con, true, 0, 0);
			REQUIRE(GetReclusterWALFileSize(path) == wal.previous_flush_end);
		}
		DeleteDatabase(path);
	}
}

TEST_CASE("Recluster WAL recovery rolls back replacement ownership after commit failure",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_recovery_commit_failure.db");
	auto wal = CreateCommittedReclusterWAL(path, true);
	auto wal_size = GetReclusterWALFileSize(path);
	DBConfig config;
	config.SetOptionByName("debug_force_commit_failure", true);
	string error;
	try {
		DuckDB reopened(path, &config);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	REQUIRE(error.find("Forced commit failure") != string::npos);
	REQUIRE(GetReclusterWALFileSize(path) == wal_size);

	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &attached = GetReclusterDatabase(db, con);
		REQUIRE(attached.GetReclusterManager().GetWALBlockRetention().Count() == 1);
		CheckReclusterBlocksReserved(attached, wal, true);
		CheckRecoveredRecluster(con, true, 1, 1);
	}
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery ignores an invalid uncommitted manifest",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_invalid_torn_manifest.db");
	auto wal = CreateCommittedReclusterWAL(path, false);
	wal.header.manifest_checksum++;
	auto previous_flush_end = AppendReclusterWAL(path, wal.header, false, false);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		CheckRecoveredRecluster(con, false, 1, 1);
		REQUIRE(GetReclusterWALFileSize(path) == previous_flush_end);
	}
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery skips a checkpointed replacement", "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_skip_recovery.db");
	auto wal = CreateCommittedReclusterWAL(path, false);
	AppendReclusterWAL(path, wal.header, true, true);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &attached = GetReclusterDatabase(db, con);
		auto &retention = attached.GetReclusterManager().GetWALBlockRetention();
		REQUIRE(retention.Count() == 1);
		CheckReclusterBlocksReserved(attached, wal, true);
		CheckRecoveredRecluster(con, false, 1, 0);
		REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
		REQUIRE(retention.Count() == 0);
		CheckReclusterBlocksReserved(attached, wal, false);
	}
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery rejects an unknown persistent table ID",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_unknown_table.db");
	auto committed_wal = CreateCommittedReclusterWAL(path, false);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET auto_recluster=false"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE tbl"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		auto database_name = DatabaseManager::GetDefaultDatabase(*con.context);
		auto attached = db.instance->GetDatabaseManager().GetDatabase(database_name);
		REQUIRE(attached);
		auto wal_lock = attached->GetStorageManager().GetWALLock();
		auto wal = attached->GetStorageManager().GetWAL();
		REQUIRE(wal);
		wal->WriteRecluster(committed_wal.header);
		wal->Flush();
	}
	string error;
	try {
		DuckDB reopened(path);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	REQUIRE(error.find("unknown persistent table ID") != string::npos);
	DeleteDatabase(path);
}

TEST_CASE("Recluster WAL recovery rejects a mismatched manifest envelope",
          "[storage][recluster_finalize][recluster_wal]") {
	auto path = TestCreatePath("recluster_finalize_corrupt_manifest.db");
	auto wal = CreateCommittedReclusterWAL(path, false);
	wal.header.manifest_checksum++;
	AppendReclusterWAL(path, wal.header, true, true);
	string error;
	try {
		DuckDB reopened(path);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	REQUIRE(error.find("header does not match its replacement manifest") != string::npos);
	DeleteDatabase(path);
}
