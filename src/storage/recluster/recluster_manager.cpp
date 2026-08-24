#include "duckdb/storage/recluster/recluster_manager.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_commit.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

ReclusterManager::ReclusterManager(AttachedDatabase &db_p)
    : db(db_p), wal_block_retention(db_p), retirement_registry(db_p) {
}

uint64_t ReclusterManager::AllocateInitializationToken() {
	auto result = next_initialization_token.fetch_add(1);
	if (result == 0 || result == NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("Recluster table initialization token space is exhausted");
	}
	return result;
}

uint64_t ReclusterManager::BeginCheckpoint() {
	auto result = next_checkpoint_number.fetch_add(1);
	if (result == 0 || result == NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("Recluster checkpoint number space is exhausted");
	}
	return result;
}

shared_ptr<TableReclusterState> ReclusterManager::SynchronizeTable(DuckTableEntry &table) {
	if (!table.HasSortHistory()) {
		return nullptr;
	}
	auto &sort_metadata = *table.GetSortMetadata();
	auto &storage = table.GetStorage();
	auto &table_info = *storage.GetDataTableInfo();
	auto state = table_info.GetReclusterState();
	if (!state) {
		state = table_info.GetOrCreateReclusterState(AllocateInitializationToken());
	}
	auto storage_generation_id = storage.GetRowGroupCollection()->GetStorageGenerationId();
	auto accept_new_tasks = sort_metadata.IsEnabled() && !db.IsReadOnly();
	state->SynchronizeCatalog(sort_metadata.table_id, sort_metadata.current_sort_order_id, storage_generation_id,
	                          accept_new_tasks);

	lock_guard<mutex> guard(queue_lock);
	if (accept_new_tasks) {
		tables[sort_metadata.table_id] = state;
	} else {
		tables.erase(sort_metadata.table_id);
	}
	return state;
}

optional<PendingCheckpointTableState> ReclusterManager::PrepareCheckpoint(DuckTableEntry &table,
                                                                          uint64_t checkpoint_number) {
	if (!table.SortEnabled()) {
		SynchronizeTable(table);
		return nullopt;
	}
	auto state = SynchronizeTable(table);
	auto &storage = table.GetStorage();
	auto snapshot =
	    BuildCheckpointLayoutSnapshot(*storage.GetRowGroupCollection(), storage.Columns(), checkpoint_number);
	if (!snapshot) {
		return nullopt;
	}

	PendingCheckpointTableState result;
	result.table_id = table.GetSortMetadata()->table_id;
	result.sort_order_id = table.GetSortMetadata()->current_sort_order_id;
	result.initialization_token = state->GetInitializationToken();
	result.state = std::move(state);
	result.storage = storage.shared_from_this();
	result.candidate_snapshot = std::move(*snapshot);
	return result;
}

void ReclusterManager::OnCheckpointSuccess(vector<PendingCheckpointTableState> &&states) noexcept {
	for (auto &pending : states) {
		if (!pending.storage || !pending.storage->IsMainTable()) {
			continue;
		}
		auto current_state = pending.storage->GetDataTableInfo()->GetReclusterState();
		if (current_state.get() != pending.state.get() ||
		    current_state->GetInitializationToken() != pending.initialization_token ||
		    current_state->GetTableId() != pending.table_id) {
			continue;
		}
		auto storage_generation_id = pending.storage->GetRowGroupCollection()->GetStorageGenerationId();
		if (storage_generation_id != pending.candidate_snapshot.storage_generation_id) {
			continue;
		}
		pending.state->TryInstallCheckpointSnapshot(pending.sort_order_id, storage_generation_id,
		                                            std::move(pending.candidate_snapshot));
	}
}

template <class CALLBACK>
static void ScanReclusterTables(AttachedDatabase &db, CALLBACK &&callback) {
	auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
	catalog.ScanSchemas([&](SchemaCatalogEntry &schema) {
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::TABLE_ENTRY) {
				callback(entry.Cast<DuckTableEntry>());
			}
		});
	});
}

void ReclusterManager::InitializeCheckpointTables() {
	ScanReclusterTables(db, [&](DuckTableEntry &table) {
		if (!table.SortEnabled()) {
			return;
		}
		auto state = SynchronizeTable(table);
		auto &storage = table.GetStorage();
		auto snapshot = BuildCheckpointLayoutSnapshot(*storage.GetRowGroupCollection(), storage.Columns(), 0);
		if (!snapshot) {
			return;
		}
		state->TryInstallCheckpointSnapshot(table.GetSortMetadata()->current_sort_order_id,
		                                    snapshot->storage_generation_id, std::move(*snapshot));
	});
}

void ReclusterManager::SynchronizeLoadedCatalog() {
	{
		lock_guard<mutex> guard(queue_lock);
		tables.clear();
	}
	ScanReclusterTables(db, [&](DuckTableEntry &table) {
		if (table.HasSortHistory()) {
			SynchronizeTable(table);
		}
	});
}

static vector<idx_t> BindReclusterSortIndexes(const vector<ColumnDefinition> &columns,
                                              const SortOrderDefinition &definition) {
	vector<idx_t> result;
	result.reserve(definition.columns.size());
	for (auto &sort_column : definition.columns) {
		optional_idx physical_index;
		for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
			if (columns[column_index].PersistentColumnId() == sort_column.column_id) {
				physical_index = column_index;
				break;
			}
		}
		if (!physical_index.IsValid()) {
			throw InternalException("Current SORTED BY definition references a missing storage column ID");
		}
		result.push_back(physical_index.GetIndex());
	}
	return result;
}

static recluster_task_id_t GenerateReclusterTaskId(TableReclusterState &state) {
	for (idx_t attempt = 0; attempt < 8; attempt++) {
		auto task_id = UUID::GenerateRandomUUID();
		if (task_id != recluster_task_id_t(0, 0) && !state.GetTask(task_id)) {
			return task_id;
		}
	}
	throw InternalException("Failed to allocate a unique recluster task ID");
}

ReclusterTaskStartResult ReclusterManager::TryStartTask(DuckTableEntry &table, const ReclusterCandidate &candidate) {
	auto &storage = table.GetStorage();
	if (&storage.GetDataTableInfo()->GetDB() != &db) {
		throw InternalException("Cannot start a recluster task through a different attached database");
	}
	auto state = storage.GetDataTableInfo()->GetReclusterState();
	if (!state) {
		return {};
	}

	shared_ptr<RangeTask> task;
	while (true) {
		auto table_write_lock = storage.GetDataTableInfo()->GetExclusiveReclusterWriteLock();
		auto layout_lock = TryGetSharedLayoutPublishLock();
		if (!layout_lock) {
			table_write_lock.reset();
			auto wait_for_checkpoint = GetSharedLayoutPublishLock();
			(void)wait_for_checkpoint;
			continue;
		}

		if (!storage.IsMainTable() || !table.SortEnabled()) {
			return {};
		}
		auto &metadata = *table.GetSortMetadata();
		auto definition = metadata.GetCurrent();
		if (metadata.table_id != state->GetTableId() || !definition ||
		    definition->sort_order_id != candidate.sort_order_id) {
			return {};
		}
		auto validated =
		    RevalidateReclusterCandidate(*storage.GetRowGroupCollection(), storage.Columns(), *state, candidate);
		if (!validated) {
			return {};
		}

		auto physical_sort_indexes = BindReclusterSortIndexes(storage.Columns(), *definition);
		auto task_id = GenerateReclusterTaskId(*state);
		auto task_context = make_uniq<ReclusterTaskContext>(
		    metadata.table_id, state->GetInitializationToken(), std::move(*validated), *definition,
		    std::move(physical_sort_indexes), storage.shared_from_this(), db);
		task = make_shared_ptr<RangeTask>(task_id, std::move(task_context));
		if (!state->TryRegisterTask(task)) {
			return {ReclusterTaskStartStatus::RANGE_UNAVAILABLE, nullptr};
		}
		break;
	}

	if (!task->TryAdvance(RangeTaskState::STARTING, RangeTaskState::PREPARING)) {
		task->TryEnterCancelling();
		task->TryDetach();
		task->GetTaskContext().CloseSnapshot();
		state->RemoveTask(task->GetTaskId());
		return {ReclusterTaskStartStatus::CANCELLED, nullptr};
	}
	return {ReclusterTaskStartStatus::STARTED, std::move(task)};
}

static bool CheckFinalizePhysicalColumns(const DataTable &storage, const ReplacementManifest &manifest) {
	if (storage.Columns().size() != manifest.physical_columns.size()) {
		return false;
	}
	for (idx_t column_index = 0; column_index < storage.Columns().size(); column_index++) {
		auto &column = storage.Columns()[column_index];
		auto &manifest_column = manifest.physical_columns[column_index];
		if (column.PersistentColumnId() != manifest_column.column_id || column.Type() != manifest_column.type) {
			return false;
		}
	}
	return true;
}

static bool CheckFinalizeManifest(const ReplacementManifest &manifest) {
	try {
		manifest.Validate();
		return true;
	} catch (SerializationException &) {
		return false;
	}
}

static bool CheckFinalizeCheckpoint(const TableReclusterState &state, const ReclusterCandidate &candidate) {
	auto checkpoint = state.GetLastCheckpoint();
	if (!checkpoint || checkpoint->storage_generation_id != candidate.storage_generation_id) {
		return false;
	}
	idx_t checkpoint_index = 0;
	for (auto &expected : candidate.expected_row_groups) {
		while (checkpoint_index < checkpoint->row_groups.size() &&
		       checkpoint->row_groups[checkpoint_index].start < expected.start) {
			checkpoint_index++;
		}
		if (checkpoint_index >= checkpoint->row_groups.size() ||
		    !(checkpoint->row_groups[checkpoint_index] == expected)) {
			return false;
		}
		checkpoint_index++;
	}
	return true;
}

static bool CheckFinalizeInputs(RowGroupCollection &collection, const vector<ColumnDefinition> &columns,
                                const ReclusterCandidate &candidate) {
	auto snapshot = collection.GetCurrentSnapshot();
	if (snapshot.kind != RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT) {
		return false;
	}
	for (auto &patch : snapshot.layout->patches) {
		if (patch->range.Overlaps(candidate.range)) {
			return false;
		}
	}
	for (auto &expected : candidate.expected_row_groups) {
		LayoutRowGroupEntry current;
		if (!snapshot.Lookup(expected.start, current) || current.row_start != expected.start ||
		    current.GetRowEnd() != expected.start + NumericCast<row_t>(expected.count)) {
			return false;
		}
		auto identity = ComputeRowGroupPhysicalIdentityV1(*current.row_group, current.row_start, columns);
		if (!identity || !(*identity == expected)) {
			return false;
		}
	}
	return true;
}

static vector<row_t> BuildFinalDeleteRowIds(RangeTask &task, const ReclusterDeleteJournalScan &scan) {
	vector<row_t> result;
	result.reserve(scan.committed_rowid_count);
	auto &remap = task.GetTaskContext().GetRowIdRemap();
	for (auto &slot_ref : scan.slots) {
		auto &slot = slot_ref.get();
		if (slot.GetState() != DeleteSlotState::COMMITTED) {
			continue;
		}
		for (auto old_row_id : slot.GetOldRowIds()) {
			auto new_row_id = remap.GetNewRowId(old_row_id);
			if (new_row_id != INVALID_REMAP_ROW_ID) {
				result.push_back(new_row_id);
			}
		}
	}
	std::sort(result.begin(), result.end());
	result.erase(std::unique(result.begin(), result.end()), result.end());
	return result;
}

static ReclusterTaskFinalizeStatus CancelFinalizeTask(TableReclusterState &state, const shared_ptr<RangeTask> &task) {
	if (task->TryEnterCancelling()) {
		if (task->HasTaskContext() && task->GetTaskContext().HasOutput()) {
			task->GetTaskContext().GetOutput().Abort();
		}
		if (task->HasTaskContext()) {
			task->GetTaskContext().CloseSnapshot();
		}
		task->TryDetach();
	} else {
		task->TryFail();
	}
	state.RemoveTask(task->GetTaskId());
	return ReclusterTaskFinalizeStatus::CANCELLED;
}

static ReclusterTaskFinalizeStatus FailFinalizeTask(TableReclusterState &state, const shared_ptr<RangeTask> &task) {
	if (task->HasTaskContext() && task->GetTaskContext().HasOutput()) {
		task->GetTaskContext().GetOutput().Abort();
	}
	if (task->HasTaskContext()) {
		task->GetTaskContext().CloseSnapshot();
	}
	task->TryFail();
	state.RemoveTask(task->GetTaskId());
	return ReclusterTaskFinalizeStatus::STALE_TASK;
}

ReclusterTaskFinalizeStatus ReclusterManager::FinalizeTask(DuckTableEntry &table, const shared_ptr<RangeTask> &task) {
	if (!task || !task->HasTaskContext()) {
		return ReclusterTaskFinalizeStatus::STALE_TASK;
	}
	auto storage = task->GetTaskContext().GetStorage();
	if (!storage || &storage->GetDataTableInfo()->GetDB() != &db) {
		throw InternalException("Cannot finalize a recluster task through a different attached database");
	}
	auto state = storage->GetDataTableInfo()->GetReclusterState();
	if (!state) {
		return ReclusterTaskFinalizeStatus::STALE_TASK;
	}

	auto finalize_lock = state->LockFinalize();
	if (task->IsCancelRequested() || task->IsPublishForbidden()) {
		return CancelFinalizeTask(*state, task);
	}
	if (!state->OwnsTask(task) || !task->TryAdvance(RangeTaskState::PREPARED, RangeTaskState::FINALIZING)) {
		return ReclusterTaskFinalizeStatus::STALE_TASK;
	}

	bool entered_committing = false;
	bool undo_pushed = false;
	try {
		auto table_write_gate = storage->GetDataTableInfo()->GetExclusiveReclusterWriteLock();
		auto layout_lock = TryGetSharedLayoutPublishLock();
		while (!layout_lock) {
			table_write_gate.reset();
			auto wait_for_checkpoint = GetSharedLayoutPublishLock();
			wait_for_checkpoint.reset();
			table_write_gate = storage->GetDataTableInfo()->GetExclusiveReclusterWriteLock();
			layout_lock = TryGetSharedLayoutPublishLock();
		}

		if (task->IsCancelRequested() || task->IsPublishForbidden()) {
			return CancelFinalizeTask(*state, task);
		}
		auto &task_context = task->GetTaskContext();
		auto &candidate = task_context.GetCandidate();
		auto &output = task_context.GetOutput();
		auto &manifest = output.GetManifest();
		auto current_layout = storage->GetRowGroupCollection()->GetCurrentLayout();
		auto current_definition = table.GetSortMetadata() ? table.GetSortMetadata()->GetCurrent() : nullptr;
		if (!storage->IsMainTable() || &table.GetStorage() != storage.get() || !table.SortEnabled() ||
		    !current_definition || !(*current_definition == task_context.GetSortDefinition()) ||
		    table.GetSortMetadata()->table_id != task_context.GetTableId() ||
		    state->GetInitializationToken() != task_context.GetInitializationToken() || !state->OwnsTask(task) ||
		    state->GetCurrentSortOrderId() != candidate.sort_order_id ||
		    state->GetCurrentStorageGenerationId() != candidate.storage_generation_id ||
		    storage->GetRowGroupCollection()->GetStorageGenerationId() != candidate.storage_generation_id ||
		    !current_layout || current_layout->patches.size() >= MAX_LAYOUT_PATCHES_PER_CHECKPOINT ||
		    storage->GetDataTableInfo()->GetSortStorage().current_layout_version.load() !=
		        current_layout->layout_version ||
		    manifest.header.task_id != task->GetTaskId() || manifest.header.table_id != task_context.GetTableId() ||
		    manifest.header.prepared_layout_version != candidate.layout_version ||
		    manifest.header.sort_order_id != candidate.sort_order_id ||
		    manifest.header.input_range.start != task->GetRange().start ||
		    manifest.header.input_range.end != task->GetRange().end ||
		    manifest.old_groups != candidate.expected_row_groups ||
		    manifest.sort_columns != task_context.GetSortDefinition().columns ||
		    !CheckFinalizePhysicalColumns(*storage, manifest) || !CheckFinalizeManifest(manifest) ||
		    !CheckFinalizeCheckpoint(*state, candidate) ||
		    !CheckFinalizeInputs(*storage->GetRowGroupCollection(), storage->Columns(), candidate)) {
			return FailFinalizeTask(*state, task);
		}

		auto &journal_limits = task->GetDeleteJournalLimits();
		auto final_scan = task->ScanResolvedDeletes(manifest.header.last_applied_delete_sequence,
		                                            journal_limits.max_slots, journal_limits.max_rowids);
		if (final_scan.blocked_by_reserved || final_scan.limit_exceeded ||
		    final_scan.resolved_through != task->GetLatestDeleteSequence()) {
			if (!task->TryAdvance(RangeTaskState::FINALIZING, RangeTaskState::PREPARED)) {
				return CancelFinalizeTask(*state, task);
			}
			return ReclusterTaskFinalizeStatus::RETRY;
		}
		auto final_deleted_new_rowids = BuildFinalDeleteRowIds(*task, final_scan);

		auto patch = make_shared_ptr<LayoutPatch>();
		patch->task_id = task->GetTaskId();
		patch->range = task->GetRange();
		patch->sort_order_id = output.GetSortOrderId();
		patch->run_id = output.GetRunId();
		patch->replaced_physical_rows = candidate.input_physical_rows;
		patch->replacement_physical_rows = output.GetRowCount();
		patch->replacement_groups = output.GetRowGroups();
		auto pending_layout = storage->GetRowGroupCollection()->BuildPendingPatchedLayout(std::move(patch));
		auto commit_info =
		    make_uniq<ReclusterCommitInfo>(task, state, storage, current_layout, std::move(pending_layout),
		                                   std::move(final_deleted_new_rowids), final_scan.resolved_through);

		if (!task->TryEnterCommitting()) {
			return CancelFinalizeTask(*state, task);
		}
		entered_committing = true;
		Connection maintenance_connection(db.GetDatabase());
		auto &maintenance_context = maintenance_connection.context->transaction;
		maintenance_context.BeginTransaction();
		MetaTransaction::Get(*maintenance_connection.context).ModifyDatabase(db, DatabaseModificationType());
		auto &maintenance_transaction = DuckTransaction::Get(*maintenance_connection.context, db);
		maintenance_transaction.SetIsReclusterMaintenanceTransaction();
		maintenance_transaction.PushRecluster(std::move(commit_info));
		undo_pushed = true;
		maintenance_context.Commit();
		if (task->GetState() != RangeTaskState::PUBLISHED) {
			throw InternalException("Recluster maintenance transaction did not publish its task");
		}
		return ReclusterTaskFinalizeStatus::PUBLISHED;
	} catch (...) {
		if (!entered_committing) {
			if (!task->TryAdvance(RangeTaskState::FINALIZING, RangeTaskState::PREPARED)) {
				FailFinalizeTask(*state, task);
			}
		} else if (!undo_pushed && task->GetState() == RangeTaskState::COMMITTING) {
			FailFinalizeTask(*state, task);
		}
		throw;
	}
}

} // namespace duckdb
