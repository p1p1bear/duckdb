#include "duckdb/storage/recluster/recluster_manager.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_commit.hpp"
#include "duckdb/storage/recluster/recluster_delete_catchup.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_output_writer.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <chrono>
#include <exception>
#include <thread>

namespace duckdb {

ReclusterManager::ReclusterManager(AttachedDatabase &db_p)
    : db(db_p), wal_block_retention(db_p), retirement_registry(db_p) {
	InitializeAutoScheduler();
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
	bool installed_snapshot = false;
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
		installed_snapshot |= pending.state->TryInstallCheckpointSnapshot(pending.sort_order_id, storage_generation_id,
		                                                                  std::move(pending.candidate_snapshot));
	}
	if (installed_snapshot) {
		RequestAutoRecluster();
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

const char *ReclusterExplicitStateToString(ReclusterExplicitState state) {
	switch (state) {
	case ReclusterExplicitState::COMPLETE:
		return "COMPLETE";
	case ReclusterExplicitState::BUDGET_EXHAUSTED:
		return "BUDGET_EXHAUSTED";
	case ReclusterExplicitState::NO_ELIGIBLE_RANGE:
		return "NO_ELIGIBLE_RANGE";
	case ReclusterExplicitState::ALREADY_RUNNING:
		return "ALREADY_RUNNING";
	case ReclusterExplicitState::FAILED:
		return "FAILED";
	default:
		throw InternalException("Unknown explicit recluster state");
	}
}

class ReclusterByteEstimateCollector : public BlockIdVisitor {
public:
	void Visit(block_id_t block_id) override {
		if (block_id >= 0) {
			blocks.insert(block_id);
		}
	}

	void Add(const MetaBlockPointer &pointer) {
		if (pointer.IsValid()) {
			Visit(pointer.GetBlockId());
		}
	}

	void Add(RowGroup &row_group) {
		for (idx_t column_index = 0; column_index < row_group.GetColumnCount(); column_index++) {
			row_group.GetRawColumnData(column_index).VisitBlockIds(*this);
		}
		for (auto &pointer : row_group.GetColumnStartPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetExtraMetadataBlockPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetDeleteStartPointers()) {
			Add(pointer);
		}
		for (auto &pointer : row_group.GetLoadedDeleteStoragePointers()) {
			Add(pointer);
		}
	}

	idx_t GetByteSize(const BlockManager &block_manager) const {
		auto block_size = block_manager.GetBlockAllocSize();
		if (block_size != 0 && blocks.size() > NumericLimits<idx_t>::Maximum() / block_size) {
			return NumericLimits<idx_t>::Maximum();
		}
		return blocks.size() * block_size;
	}

private:
	unordered_set<block_id_t> blocks;
};

static idx_t AddReclusterBytes(idx_t left, idx_t right) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		return NumericLimits<idx_t>::Maximum();
	}
	return left + right;
}

static idx_t EstimateReclusterRangeBytes(DataTable &storage, const RowGroupRange &range) {
	ReclusterByteEstimateCollector collector;
	LayoutRowGroupCursor cursor(storage.GetRowGroupCollection()->GetCurrentSnapshot(), range);
	LayoutRowGroupEntry entry;
	while (cursor.Next(entry)) {
		collector.Add(*entry.row_group);
	}
	return collector.GetByteSize(storage.GetTableIOManager().GetBlockManagerForRowData());
}

static idx_t EstimateReclusterOutputBytes(DataTable &storage, const vector<block_id_t> &block_ids) {
	ReclusterByteEstimateCollector collector;
	for (auto block_id : block_ids) {
		collector.Visit(block_id);
	}
	return collector.GetByteSize(storage.GetTableIOManager().GetBlockManagerForRowData());
}

static idx_t EstimateRemainingReclusterBytes(DataTable &storage, TableReclusterState &state) {
	auto current_sort_order = state.GetCurrentSortOrderId();
	if (current_sort_order == INVALID_SORT_ORDER_ID) {
		return 0;
	}

	vector<LayoutRowGroupEntry> row_groups;
	bool has_old_organization = false;
	idx_t current_run_count = 0;
	sort_run_id_t previous_run_id = INVALID_SORT_RUN_ID;
	bool previous_was_current = false;
	LayoutRowGroupCursor cursor(storage.GetRowGroupCollection()->GetCurrentSnapshot());
	LayoutRowGroupEntry entry;
	while (cursor.Next(entry)) {
		auto metadata = entry.row_group->GetSortMetadata();
		auto is_current = metadata.sort_order_id == current_sort_order;
		if (!is_current) {
			has_old_organization = true;
		} else if (!previous_was_current || metadata.run_id != previous_run_id) {
			current_run_count++;
		}
		previous_was_current = is_current;
		previous_run_id = is_current ? metadata.run_id : INVALID_SORT_RUN_ID;
		row_groups.push_back(entry);
	}

	ReclusterByteEstimateCollector collector;
	auto include_current_runs = current_run_count > 1 || (has_old_organization && current_run_count > 0);
	for (auto &row_group_entry : row_groups) {
		auto &row_group = *row_group_entry.row_group;
		auto is_current = row_group.GetSortMetadata().sort_order_id == current_sort_order;
		auto physical_rows = row_group.count.load();
		auto live_rows = row_group.GetCommittedRowCount();
		auto needs_delete_cleanup =
		    physical_rows > 0 && live_rows < physical_rows &&
		    static_cast<long double>(physical_rows - live_rows) >= static_cast<long double>(physical_rows) * 0.25;
		if (!is_current || include_current_runs || needs_delete_cleanup) {
			collector.Add(row_group);
		}
	}
	return collector.GetByteSize(storage.GetTableIOManager().GetBlockManagerForRowData());
}

ReclusterTaskStartResult ReclusterManager::TryStartTask(DuckTableEntry &table, const ReclusterCandidate &candidate,
                                                        optional_ptr<ClientContext> driver_context) {
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
		    std::move(physical_sort_indexes), storage.shared_from_this(), db, driver_context);
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

static void CancelExplicitTask(TableReclusterState &state, const shared_ptr<RangeTask> &task) {
	if (!task || !state.OwnsTask(task)) {
		return;
	}
	std::exception_ptr cleanup_error;
	task->RequestCancel();
	if (task->TryEnterCancelling()) {
		if (task->HasTaskContext() && task->GetTaskContext().HasOutput()) {
			try {
				task->GetTaskContext().GetOutput().Abort();
			} catch (...) {
				cleanup_error = std::current_exception();
			}
		}
		if (task->HasTaskContext()) {
			try {
				task->GetTaskContext().CloseSnapshot();
			} catch (...) {
				if (!cleanup_error) {
					cleanup_error = std::current_exception();
				}
			}
		}
		if (!task->TryDetach() && !cleanup_error) {
			cleanup_error = std::make_exception_ptr(InternalException("Failed to detach an explicit recluster task"));
		}
	} else if (!task->IsFinished()) {
		task->TryFail();
	}
	state.RemoveTask(task->GetTaskId());
	if (cleanup_error) {
		std::rethrow_exception(cleanup_error);
	}
}

static ReclusterCandidateLimits ExplicitCandidateLimits(DataTable &storage, idx_t max_row_groups) {
	auto row_group_size = storage.GetRowGroupSize();
	auto max_rows = row_group_size > NumericLimits<idx_t>::Maximum() / max_row_groups ? NumericLimits<idx_t>::Maximum()
	                                                                                  : row_group_size * max_row_groups;
	return {max_rows, max_row_groups, 4, 0.25};
}

ReclusterExplicitResult ReclusterManager::RunExplicit(ClientContext &context, const QualifiedName &table_name,
                                                      const ReclusterExplicitOptions &options) {
	if (options.max_bytes == 0 || options.max_tasks == 0) {
		throw InvalidInputException("Explicit recluster budgets must be greater than zero");
	}
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException("CALL recluster cannot run inside an explicit transaction");
	}
	if (db.IsReadOnly()) {
		throw PermissionException("Cannot recluster a read-only database");
	}

	auto &initial_table = Catalog::GetEntry<DuckTableEntry>(context, table_name);
	if (&initial_table.GetStorage().GetDataTableInfo()->GetDB() != &db) {
		throw InternalException("Cannot explicitly recluster a table through a different attached database");
	}
	if (!initial_table.SortEnabled()) {
		throw InvalidInputException("Table %s does not have SORTED BY enabled", table_name.ToString());
	}
	auto storage = initial_table.GetStorage().shared_from_this();
	auto state = storage->GetDataTableInfo()->GetReclusterState();
	if (!state) {
		state = SynchronizeTable(initial_table);
	}
	if (!state) {
		throw InternalException("SORTED BY table has no recluster state");
	}

	ReclusterExplicitResult result;
	result.table_name = table_name.ToString();
	result.state = ReclusterExplicitState::COMPLETE;
	auto explicit_lock = state->TryLockExplicit();
	if (!explicit_lock.owns_lock() || state->GetTaskCount() != 0) {
		result.state = ReclusterExplicitState::ALREADY_RUNNING;
		result.message = "another maintenance task is already active for this table";
		result.remaining_recluster_bytes = EstimateRemainingReclusterBytes(*storage, *state);
		return result;
	}
	auto ddl_coordination_lock = storage->GetDataTableInfo()->GetReclusterDDLCoordinationLock();

	bool checkpoint_created = false;
	idx_t stale_attempts = 0;
	while (result.tasks_completed < options.max_tasks) {
		context.InterruptCheck();
		auto &table = Catalog::GetEntry<DuckTableEntry>(context, table_name);
		if (&table.GetStorage() != storage.get() || !table.SortEnabled() ||
		    storage->GetDataTableInfo()->GetReclusterState().get() != state.get()) {
			result.state = ReclusterExplicitState::FAILED;
			result.message = "the table definition changed while recluster was running";
			break;
		}

		auto remaining_budget = options.max_bytes - result.input_bytes;
		idx_t max_row_groups = 32;
		ReclusterCandidateSelection selection;
		idx_t candidate_bytes = 0;
		while (true) {
			auto limits = ExplicitCandidateLimits(*storage, max_row_groups);
			selection = SelectReclusterCandidate(*storage->GetRowGroupCollection(), storage->Columns(), *state, limits);
			if (!selection.candidate) {
				break;
			}
			candidate_bytes = EstimateReclusterRangeBytes(*storage, selection.candidate->range);
			if (candidate_bytes <= remaining_budget) {
				break;
			}
			if (selection.candidate->row_group_count <= 1) {
				selection.candidate.reset();
				break;
			}
			max_row_groups = selection.candidate->row_group_count - 1;
		}

		if (!selection.candidate) {
			result.remaining_recluster_bytes = EstimateRemainingReclusterBytes(*storage, *state);
			if (candidate_bytes > remaining_budget || result.input_bytes >= options.max_bytes) {
				result.state = ReclusterExplicitState::BUDGET_EXHAUSTED;
				result.message = "the remaining byte budget cannot admit another row group";
				break;
			}
			if (result.remaining_recluster_bytes == 0) {
				result.state = ReclusterExplicitState::COMPLETE;
				result.message = "no recluster work remains";
				break;
			}
			if (!checkpoint_created && options.create_checkpoint &&
			    selection.status == ReclusterCandidateSelectionStatus::NO_CHECKPOINTED_RANGE) {
				TransactionManager::Get(db).Checkpoint(context, false);
				checkpoint_created = true;
				continue;
			}
			result.state = ReclusterExplicitState::NO_ELIGIBLE_RANGE;
			result.message = selection.status == ReclusterCandidateSelectionStatus::RUN_EXCEEDS_TASK_LIMIT
			                     ? "the next sorted run exceeds the per-task row-group limit"
			                     : "remaining work is waiting for a successful checkpoint";
			break;
		}

		auto start = TryStartTask(table, *selection.candidate, context);
		if (start.status != ReclusterTaskStartStatus::STARTED || !start.task) {
			if (start.status == ReclusterTaskStartStatus::RANGE_UNAVAILABLE) {
				result.state = ReclusterExplicitState::ALREADY_RUNNING;
				result.message = "the selected range is already being maintained";
				break;
			}
			if (++stale_attempts < 8) {
				continue;
			}
			result.state = ReclusterExplicitState::FAILED;
			result.message = "the table changed repeatedly while selecting recluster work";
			break;
		}
		stale_attempts = 0;

		try {
			ReclusterOutputWriter writer(*start.task);
			writer.Write();
			if (!start.task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES)) {
				throw InternalException("Failed to begin explicit recluster DELETE catch-up");
			}
			while (start.task->GetState() == RangeTaskState::CATCHING_UP_DELETES) {
				ReclusterDeleteCatchup catchup(*start.task);
				catchup.Run();
			}
			auto task_output_bytes =
			    EstimateReclusterOutputBytes(*storage, start.task->GetTaskContext().GetOutput().GetBlockIds());

			auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
			while (true) {
				context.InterruptCheck();
				auto finalize_status = FinalizeTask(table, start.task);
				if (finalize_status == ReclusterTaskFinalizeStatus::PUBLISHED) {
					break;
				}
				if (finalize_status != ReclusterTaskFinalizeStatus::RETRY) {
					throw IOException("Explicit recluster task could not publish its replacement layout");
				}
				if (std::chrono::steady_clock::now() >= retry_deadline) {
					throw IOException("Explicit recluster timed out waiting for concurrent DELETE transactions");
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			result.tasks_completed++;
			result.input_bytes = AddReclusterBytes(result.input_bytes, candidate_bytes);
			result.output_bytes = AddReclusterBytes(result.output_bytes, task_output_bytes);
		} catch (...) {
			auto error = std::current_exception();
			CancelExplicitTask(*state, start.task);
			try {
				std::rethrow_exception(error);
			} catch (InterruptException &) {
				throw;
			} catch (FatalException &) {
				throw;
			} catch (DataCorruptionException &) {
				throw;
			} catch (SerializationException &) {
				throw;
			} catch (InternalException &) {
				throw;
			} catch (std::exception &ex) {
				result.state = ReclusterExplicitState::FAILED;
				result.message = ex.what();
				break;
			}
		}
	}

	result.remaining_recluster_bytes = EstimateRemainingReclusterBytes(*storage, *state);
	if (result.state == ReclusterExplicitState::FAILED || result.state == ReclusterExplicitState::ALREADY_RUNNING ||
	    result.state == ReclusterExplicitState::NO_ELIGIBLE_RANGE) {
		return result;
	}
	if (result.remaining_recluster_bytes == 0) {
		result.state = ReclusterExplicitState::COMPLETE;
		result.message = "no recluster work remains";
	} else if (result.tasks_completed >= options.max_tasks) {
		result.state = ReclusterExplicitState::BUDGET_EXHAUSTED;
		result.message = "the task budget was exhausted";
	} else if (result.input_bytes >= options.max_bytes) {
		result.state = ReclusterExplicitState::BUDGET_EXHAUSTED;
		result.message = "the byte budget was exhausted";
	}
	return result;
}

} // namespace duckdb
