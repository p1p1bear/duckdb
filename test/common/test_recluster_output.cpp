#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
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
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
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
