#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_commit.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <algorithm>
#include <condition_variable>
#include <exception>

namespace duckdb {

enum class HeldTableGateMode : uint8_t { ACQUIRING_SHARED, ACQUIRING_EXCLUSIVE, SHARED, EXCLUSIVE, FAILED };

struct HeldTableGate {
	HeldTableGateMode mode;
	std::condition_variable ready;
	unique_ptr<StorageLockKey> handle;
	std::exception_ptr failure;
};

enum class HeldDDLCoordinationState : uint8_t { ACQUIRING, HELD, FAILED };

struct HeldDDLCoordination {
	HeldDDLCoordinationState state;
	std::condition_variable ready;
	unique_ptr<StorageLockKey> handle;
	std::exception_ptr failure;
};

enum class ReclusterDeleteTransactionState : uint8_t { RECORDING, PREPARING, PREPARED, RESOLVED };

struct PendingTaskDeletes {
	shared_ptr<RangeTask> task;
	vector<row_t> old_rowids;
	optional_ptr<ReclusterDeleteSlot> slot;
};

struct DuckTransactionReclusterState {
	mutex lock;
	reference_map_t<DataTableInfo, shared_ptr<HeldTableGate>> table_write_locks;
	reference_map_t<DataTableInfo, shared_ptr<HeldDDLCoordination>> ddl_coordination_locks;
	ReclusterDeleteTransactionState delete_state = ReclusterDeleteTransactionState::RECORDING;
	unordered_map<recluster_task_id_t, PendingTaskDeletes> pending_deletes;
	bool is_maintenance_transaction = false;
	bool has_recluster_undo = false;
};

TransactionData::TransactionData(DuckTransaction &transaction_p) // NOLINT
    : transaction(&transaction_p), transaction_id(transaction_p.transaction_id), start_time(transaction_p.start_time) {
}
TransactionData::TransactionData(transaction_t transaction_id_p, transaction_t start_time_p)
    : transaction(nullptr), transaction_id(transaction_id_p), start_time(start_time_p) {
}

DuckTransaction::DuckTransaction(DuckTransactionManager &manager, ClientContext &context_p, transaction_t start_time,
                                 transaction_t transaction_id, idx_t catalog_version_p)
    : Transaction(manager, context_p), start_time(start_time), transaction_id(transaction_id), commit_id(0),
      catalog_version(catalog_version_p), awaiting_cleanup(false), undo_buffer(*this, context_p),
      storage(make_uniq<LocalStorage>(context_p, *this)) {
}

DuckTransaction::~DuckTransaction() {
	ResolveReclusterDeletes(false);
	ReleaseReclusterWriteLocks();
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db.GetCatalog());
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	if (!transaction.IsDuckTransaction()) {
		throw InternalException("DuckTransaction::Get called on non-DuckDB transaction");
	}
	return transaction.Cast<DuckTransaction>();
}

DuckTransactionManager &DuckTransaction::GetTransactionManager() {
	return manager.Cast<DuckTransactionManager>();
}

LocalStorage &DuckTransaction::GetLocalStorage() {
	return *storage;
}

DuckTransactionReclusterState &DuckTransaction::GetOrCreateReclusterState() {
	lock_guard<mutex> guard(active_locks_lock);
	if (!recluster_state) {
		recluster_state = make_uniq<DuckTransactionReclusterState>();
	}
	return *recluster_state;
}

optional_ptr<DuckTransactionReclusterState> DuckTransaction::GetReclusterState() {
	lock_guard<mutex> guard(active_locks_lock);
	return recluster_state.get();
}

void DuckTransaction::PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size) {
	idx_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(idx_t);
	}

	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	auto ptr = undo_entry.GetDataMutable();
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(&entry, ptr);
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		ptr += sizeof(CatalogEntry *);
		// first store the extra data size
		Store<idx_t>(extra_data_size, ptr);
		ptr += sizeof(idx_t);
		// then copy over the actual data
		memcpy(ptr, extra_data, extra_data_size);
	}
}

void DuckTransaction::PushAttach(AttachedDatabase &db) {
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::ATTACHED_DATABASE, sizeof(AttachedDatabase *));
	auto ptr = undo_entry.GetDataMutable();
	// store the pointer to the database
	Store<CatalogEntry *>(&db, ptr);
}

void DuckTransaction::PushDelete(DuckTableEntry &table_entry, RowVersionManager &info, idx_t vector_idx, row_t rows[],
                                 idx_t count, idx_t base_row) {
	bool is_consecutive = true;
	// check if the rows are consecutive
	for (idx_t i = 0; i < count; i++) {
		if (rows[i] != row_t(i)) {
			is_consecutive = false;
			break;
		}
	}
	idx_t alloc_size = sizeof(DeleteInfo);
	if (!is_consecutive) {
		// if rows are not consecutive we need to allocate row identifiers
		alloc_size += sizeof(uint16_t) * count;
	}

	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, alloc_size);
	auto delete_info = reinterpret_cast<DeleteInfo *>(undo_entry.GetDataMutable());
	delete_info->version_info = &info;
	delete_info->vector_idx = vector_idx;
	delete_info->table = &table_entry;
	delete_info->count = count;
	delete_info->base_row = base_row;
	delete_info->is_consecutive = is_consecutive;
	if (!is_consecutive) {
		// if rows are not consecutive
		auto delete_rows = delete_info->GetRows();
		for (idx_t i = 0; i < count; i++) {
			delete_rows[i] = NumericCast<uint16_t>(rows[i]);
		}
	}
}

static void CancelTaskForDeleteJournalFailure(const shared_ptr<RangeTask> &task) noexcept {
	if (!task) {
		return;
	}
	task->DisablePublishForJournalFailure();
	task->RequestCancel();
}

void DuckTransaction::RecordReclusterDeletes(DataTableInfo &info, row_t vector_base, const row_t rows[],
                                             idx_t count) noexcept {
	if (count == 0 || !info.HasSortStorage()) {
		return;
	}
	auto state = info.GetReclusterState();
	if (!state) {
		return;
	}

	auto first_row_id = vector_base + rows[0];
	auto task = state->GetTaskForRow(first_row_id);
	if (!task || task->IsCancelRequested() || task->IsPublishForbidden() || task->IsFinished()) {
		return;
	}
	for (idx_t row_index = 0; row_index < count; row_index++) {
		if (rows[row_index] < 0 || !task->GetRange().Contains(vector_base + rows[row_index])) {
			CancelTaskForDeleteJournalFailure(task);
			return;
		}
	}

	bool cancel_task = false;
	auto &recluster = GetOrCreateReclusterState();
	{
		lock_guard<mutex> guard(recluster.lock);
		if (recluster.is_maintenance_transaction) {
			return;
		}
		if (recluster.delete_state != ReclusterDeleteTransactionState::RECORDING) {
			cancel_task = true;
		} else {
			try {
				auto entry = recluster.pending_deletes.find(task->GetTaskId());
				if (entry == recluster.pending_deletes.end()) {
					PendingTaskDeletes pending;
					pending.task = task;
					pending.old_rowids.reserve(count);
					for (idx_t row_index = 0; row_index < count; row_index++) {
						pending.old_rowids.push_back(vector_base + rows[row_index]);
					}
					recluster.pending_deletes.emplace(task->GetTaskId(), std::move(pending));
				} else if (entry->second.task.get() != task.get() || entry->second.slot ||
				           count > entry->second.old_rowids.max_size() - entry->second.old_rowids.size()) {
					cancel_task = true;
					recluster.pending_deletes.erase(entry);
				} else {
					auto &old_rowids = entry->second.old_rowids;
					old_rowids.reserve(old_rowids.size() + count);
					for (idx_t row_index = 0; row_index < count; row_index++) {
						old_rowids.push_back(vector_base + rows[row_index]);
					}
				}
			} catch (...) {
				recluster.pending_deletes.erase(task->GetTaskId());
				cancel_task = true;
			}
		}
	}
	if (cancel_task) {
		CancelTaskForDeleteJournalFailure(task);
	}
}

ErrorData DuckTransaction::PrepareReclusterCommit() noexcept {
	try {
		if (storage->HasReclusterTableStorage()) {
			for (auto &local_table : storage->GetTableStorages()) {
				if (local_table->is_dropped ||
				    local_table->GetCollection().GetTotalRows() <= local_table->deleted_rows) {
					continue;
				}
				local_table->table_ref.get().PrepareReclusterCommit(*this);
			}
		}
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}

	auto recluster = GetReclusterState();
	if (!recluster) {
		return ErrorData();
	}

	vector<reference<PendingTaskDeletes>> ordered_deletes;
	{
		lock_guard<mutex> guard(recluster->lock);
		if (recluster->delete_state != ReclusterDeleteTransactionState::RECORDING) {
			return ErrorData();
		}
		recluster->delete_state = ReclusterDeleteTransactionState::PREPARING;
		try {
			ordered_deletes.reserve(recluster->pending_deletes.size());
			for (auto &entry : recluster->pending_deletes) {
				ordered_deletes.emplace_back(entry.second);
			}
			std::sort(ordered_deletes.begin(), ordered_deletes.end(),
			          [](const reference<PendingTaskDeletes> &left, const reference<PendingTaskDeletes> &right) {
				          return left.get().task->GetTaskId() < right.get().task->GetTaskId();
			          });
		} catch (...) {
			for (auto &entry : recluster->pending_deletes) {
				CancelTaskForDeleteJournalFailure(entry.second.task);
			}
			recluster->delete_state = ReclusterDeleteTransactionState::PREPARED;
			return ErrorData();
		}
	}

	for (auto &pending_ref : ordered_deletes) {
		auto &pending = pending_ref.get();
		if (pending.task->IsCancelRequested() || pending.task->IsPublishForbidden() || pending.task->IsFinished()) {
			continue;
		}
		auto slot = pending.task->TryReserveDeleteSlot(std::move(pending.old_rowids));
		if (!slot) {
			CancelTaskForDeleteJournalFailure(pending.task);
			continue;
		}
		pending.slot = slot;
	}

	lock_guard<mutex> guard(recluster->lock);
	D_ASSERT(recluster->delete_state == ReclusterDeleteTransactionState::PREPARING);
	recluster->delete_state = ReclusterDeleteTransactionState::PREPARED;
	return ErrorData();
}

void DuckTransaction::ResolveReclusterDeletes(bool committed) noexcept {
	auto recluster = GetReclusterState();
	if (!recluster) {
		return;
	}
	lock_guard<mutex> guard(recluster->lock);
	auto target = committed ? DeleteSlotState::COMMITTED : DeleteSlotState::ABORTED;
	for (auto &entry : recluster->pending_deletes) {
		auto &pending = entry.second;
		if (pending.slot && !pending.task->ResolveDeleteSlot(*pending.slot, target)) {
			CancelTaskForDeleteJournalFailure(pending.task);
		}
	}
	recluster->pending_deletes.clear();
	recluster->delete_state = ReclusterDeleteTransactionState::RESOLVED;
}

void DuckTransaction::SetIsReclusterMaintenanceTransaction() {
	auto &recluster = GetOrCreateReclusterState();
	lock_guard<mutex> guard(recluster.lock);
	D_ASSERT(recluster.delete_state == ReclusterDeleteTransactionState::RECORDING);
	D_ASSERT(recluster.pending_deletes.empty());
	recluster.is_maintenance_transaction = true;
}

void DuckTransaction::PushRecluster(unique_ptr<ReclusterCommitInfo> info) {
	if (!info) {
		throw InternalException("Cannot push a null recluster commit");
	}
	auto &recluster_state = GetOrCreateReclusterState();
	lock_guard<mutex> guard(recluster_state.lock);
	if (!recluster_state.is_maintenance_transaction || recluster_state.has_recluster_undo || undo_buffer.ChangesMade() ||
	    storage->ChangesMade()) {
		throw InternalException("A recluster maintenance transaction must contain exactly one recluster change");
	}
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::RECLUSTER, sizeof(ReclusterUndoData));
	auto recluster = reinterpret_cast<ReclusterUndoData *>(undo_entry.GetDataMutable());
	recluster->info = info.release();
	recluster_state.has_recluster_undo = true;
}

void DuckTransaction::PushAppend(DuckTableEntry &table_entry, idx_t start_row, idx_t row_count) {
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo));
	auto append_info = reinterpret_cast<AppendInfo *>(undo_entry.GetDataMutable());
	append_info->table = &table_entry;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UndoBufferReference DuckTransaction::CreateUpdateInfo(DuckTableEntry &table_entry, idx_t type_size, idx_t entries,
                                                      idx_t row_group_start) {
	idx_t alloc_size = UpdateInfo::GetAllocSize(type_size);
	auto undo_entry = undo_buffer.CreateEntry(UndoFlags::UPDATE_TUPLE, alloc_size);
	auto &update_info = UpdateInfo::Get(undo_entry);
	UpdateInfo::Initialize(update_info, table_entry, transaction_id, row_group_start);
	return undo_entry;
}

void DuckTransaction::PushSequenceUsage(SequenceCatalogEntry &sequence, const SequenceData &data) {
	lock_guard<mutex> l(sequence_lock);
	auto entry = sequence_usage.find(sequence);
	if (entry == sequence_usage.end()) {
		auto undo_entry = undo_buffer.CreateEntry(UndoFlags::SEQUENCE_VALUE, sizeof(SequenceValue));
		auto sequence_info = reinterpret_cast<SequenceValue *>(undo_entry.GetDataMutable());
		sequence_info->entry = &sequence;
		sequence_info->usage_count = data.usage_count;
		sequence_info->counter = data.counter;
		sequence_usage.emplace(sequence, *sequence_info);
	} else {
		auto &sequence_info = entry->second.get();
		D_ASSERT(RefersToSameObject(*sequence_info.entry, sequence));
		sequence_info.usage_count = data.usage_count;
		sequence_info.counter = data.counter;
	}
}

bool DuckTransaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage->ChangesMade();
}

UndoBufferProperties DuckTransaction::GetUndoProperties() {
	auto properties = undo_buffer.GetProperties();
	properties.estimated_size += storage->EstimatedSize();
	return properties;
}

bool DuckTransaction::AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties) {
	if (is_checkpoint_transaction || properties.has_recluster) {
		return false;
	}
	if (!ChangesMade()) {
		// read-only transactions cannot trigger an automated checkpoint
		return false;
	}
	if (db.IsReadOnly()) {
		// when attaching a database in read-only mode we cannot checkpoint
		// note that attaching a database in read-only mode does NOT mean we never make changes
		// WAL replay can make changes to the database - but only in the in-memory copy of the
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	return storage_manager.AutomaticCheckpoint(properties.estimated_size);
}

bool DuckTransaction::ShouldWriteToWAL(AttachedDatabase &db) {
	if (!ChangesMade()) {
		return false;
	}
	if (db.IsSystem()) {
		return false;
	}
	if (db.GetRecoveryMode() == RecoveryMode::NO_WAL_WRITES) {
		// WAL writes are explicitly disabled
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	if (!storage_manager.HasWAL()) {
		return false;
	}
	return true;
}

ErrorData DuckTransaction::WriteToWAL(ClientContext &context, AttachedDatabase &db,
                                      unique_ptr<StorageCommitState> &commit_state) noexcept {
	ErrorData error_data;
	try {
		D_ASSERT(ShouldWriteToWAL(db));
		auto &storage_manager = db.GetStorageManager();
		auto wal = storage_manager.GetWAL();
		commit_state = storage_manager.GenStorageCommitState(*wal);

		auto &profiler = *context.client_data->profiler;
		auto commit_timer = profiler.StartTimer<MetricStorageCommitLocalStorageLatency>();
		storage->Commit(commit_state.get());
		commit_timer.EndTimer();

		auto wal_timer = profiler.StartTimer<MetricStorageWriteToWALLatency>();
		undo_buffer.WriteToWAL(*wal, commit_state.get());
		if (commit_state->HasRowGroupData()) {
			// if we have optimistically written any data AND we are writing to the WAL, we have written references to
			// optimistically written blocks
			// hence we need to ensure those optimistically written blocks are persisted
			storage_manager.GetBlockManager().FileSync();
		}
		wal_timer.EndTimer();

	} catch (std::exception &ex) {
		// Call RevertCommit() outside this try-catch as it itself may throw
		error_data = ErrorData(ex);
	}

	if (commit_state && error_data.HasError()) {
		try {
			commit_state->RevertCommit();
			commit_state.reset();
		} catch (std::exception &) {
			// Ignore this error. If we fail to RevertCommit(), just return the original exception
		}
	}

	return error_data;
}

ErrorData DuckTransaction::Commit(AttachedDatabase &db, CommitInfo &commit_info,
                                  unique_ptr<StorageCommitState> commit_state) noexcept {
	this->commit_id = commit_info.commit_id;
	if (!ChangesMade()) {
		// no need to flush anything if we made no changes
		return ErrorData();
	}
	D_ASSERT(db.IsSystem() || db.IsTemporary() || !IsReadOnly());

	UndoBuffer::IteratorState iterator_state;
	optional_ptr<BlockManager> block_manager;
	if (db.HasStorageManager()) {
		block_manager = db.GetStorageManager().GetBlockManager();
	}
	CommitDropState drop_state(block_manager);
	commit_info.drop_state = &drop_state;

	ErrorData error_data;
	try {
		storage->Commit(commit_state.get());
		undo_buffer.Commit(iterator_state, commit_info);
		drop_state.PrepareFinalize();
		if (!db.IsSystem() && !db.IsTemporary() && Settings::Get<DebugForceCommitFailureSetting>(db.GetDatabase())) {
			throw InvalidInputException("Forced commit failure (debug_force_commit_failure)");
		}
		if (commit_state) {
			// if we have written to the WAL - flush after the commit has been successful
			commit_state->FlushCommit();
		}
		drop_state.FinalizeCommit();
		return ErrorData();
	} catch (std::exception &ex) {
		// Record the error and run RevertCommit() outside this try-catch: RevertCommit() iterates the
		// undo buffer and may itself throw (e.g. Pin() failing under memory pressure), which would
		// escape this noexcept function and trigger std::terminate.
		error_data = ErrorData(ex);
	}
	if (drop_state.IrreversibleFinalizationStarted()) {
		commit_finalization_irreversible = true;
		ValidChecker::Invalidate(db.GetDatabase(),
		                         "Failed while finalizing a durable transaction commit. The database must be reopened. "
		                         "Commit finalization error: " +
		                             error_data.RawMessage());
		return error_data;
	}
	drop_state.RevertPrepared();

	try {
		undo_buffer.RevertCommit(iterator_state, this->transaction_id);
		if (!db.IsSystem() && !db.IsTemporary() &&
		    Settings::Get<DebugForceCommitRevertFailureSetting>(db.GetDatabase())) {
			throw IOException("Forced RevertCommit failure (debug_force_commit_revert_failure)");
		}
		if (commit_state) {
			// if we have written to the WAL - truncate the WAL on failure
			commit_state->RevertCommit();
		}
	} catch (std::exception &ex) {
		// If we fail to revert the commit, the database is left in an undefined state - invalidate it.
		// Record both the original commit error and the revert error so the root cause stays visible.
		ValidChecker::Invalidate(db.GetDatabase(),
		                         "Failed to revert transaction commit, database is in an undefined state. "
		                         "Original commit error: " +
		                             error_data.RawMessage() + ". RevertCommit error: " + ErrorData(ex).RawMessage());
	} catch (...) {
		// last line of defense: this is a noexcept function, nothing may escape
		ValidChecker::Invalidate(db.GetDatabase(),
		                         "Failed to revert transaction commit (unknown error), database is in an "
		                         "undefined state. Original commit error: " +
		                             error_data.RawMessage());
	}
	return error_data;
}

ErrorData DuckTransaction::Rollback() {
	try {
		storage->Rollback();
		undo_buffer.Rollback();
		return ErrorData();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
}

void DuckTransaction::Cleanup(transaction_t lowest_active_transaction) {
	undo_buffer.Cleanup(lowest_active_transaction);
}

void DuckTransaction::SetModifications(DatabaseModificationType type) {
	if (!checkpoint_lock) {
		bool require_write_lock = false;
		require_write_lock = require_write_lock || type.UpdateData();
		require_write_lock = require_write_lock || type.AlterTable();
		require_write_lock = require_write_lock || type.CreateCatalogEntry();
		require_write_lock = require_write_lock || type.DropCatalogEntry();
		require_write_lock = require_write_lock || type.Sequence();
		require_write_lock = require_write_lock || type.CreateIndex();

		if (require_write_lock) {
			// obtain a shared checkpoint lock to prevent concurrent checkpoints while this transaction is running
			checkpoint_lock = GetTransactionManager().SharedCheckpointLock();
		}
	}
	if (!vacuum_lock) {
		bool require_vacuum_lock = false;
		require_vacuum_lock = require_vacuum_lock || type.InsertData();
		require_vacuum_lock = require_vacuum_lock || type.DeleteData();

		if (require_vacuum_lock) {
			vacuum_lock = GetTransactionManager().SharedVacuumLock();
		}
	}
}

unique_ptr<StorageLockKey> DuckTransaction::TryGetCheckpointLock() {
	if (!checkpoint_lock) {
		return GetTransactionManager().TryGetCheckpointLock();
	} else {
		return GetTransactionManager().TryUpgradeCheckpointLock(*checkpoint_lock);
	}
}

shared_ptr<CheckpointLock> DuckTransaction::SharedLockTable(DataTableInfo &info) {
	unique_lock<mutex> transaction_lock(active_locks_lock);
	auto entry = active_locks.find(info);
	if (entry == active_locks.end()) {
		entry = active_locks.insert(entry, make_pair(std::ref(info), make_uniq<ActiveTableLock>()));
	}
	auto &active_table_lock = *entry->second;
	transaction_lock.unlock(); // release transaction-level lock before acquiring table-level lock
	lock_guard<mutex> table_lock(active_table_lock.checkpoint_lock_mutex);
	auto checkpoint_lock = active_table_lock.checkpoint_lock.lock();
	// check if it is expired (or has never been acquired yet)
	if (checkpoint_lock) {
		// not expired - return it
		return checkpoint_lock;
	}
	// no existing lock - obtain it
	checkpoint_lock = make_shared_ptr<CheckpointLock>(info.GetSharedLock());
	// store it for future reference
	active_table_lock.checkpoint_lock = checkpoint_lock;
	return checkpoint_lock;
}

void DuckTransaction::HoldSharedReclusterWriteLock(DataTableInfo &info) {
	HoldReclusterWriteLock(info, false);
}

void DuckTransaction::HoldExclusiveReclusterWriteLock(DataTableInfo &info) {
	HoldReclusterWriteLock(info, true);
}

void DuckTransaction::HoldReclusterDDLCoordinationLock(DataTableInfo &info) {
	auto &recluster = GetOrCreateReclusterState();
	shared_ptr<HeldDDLCoordination> coordination;
	bool try_only = false;
	{
		unique_lock<mutex> guard(recluster.lock);
		auto entry = recluster.ddl_coordination_locks.find(info);
		if (entry != recluster.ddl_coordination_locks.end()) {
			coordination = entry->second;
			while (coordination->state == HeldDDLCoordinationState::ACQUIRING) {
				coordination->ready.wait(guard);
			}
			if (coordination->state == HeldDDLCoordinationState::FAILED) {
				std::rethrow_exception(coordination->failure);
			}
			return;
		}
		for (auto &held_entry : recluster.ddl_coordination_locks) {
			if (held_entry.second->state != HeldDDLCoordinationState::HELD) {
				throw TransactionException(
				    "Transaction conflict: cannot concurrently coordinate sorted-table DDL for multiple tables");
			}
			try_only = true;
		}
		coordination = make_shared_ptr<HeldDDLCoordination>();
		coordination->state = HeldDDLCoordinationState::ACQUIRING;
		recluster.ddl_coordination_locks.emplace(std::ref(info), coordination);
	}

	unique_ptr<StorageLockKey> handle;
	std::exception_ptr failure;
	try {
		handle = try_only ? info.TryGetReclusterDDLCoordinationLock() : info.GetReclusterDDLCoordinationLock();
		if (!handle) {
			throw TransactionException(
			    "Transaction conflict: cannot immediately coordinate sorted-table DDL for another table");
		}
	} catch (...) {
		failure = std::current_exception();
	}
	{
		lock_guard<mutex> guard(recluster.lock);
		if (failure) {
			coordination->failure = failure;
			coordination->state = HeldDDLCoordinationState::FAILED;
		} else {
			coordination->handle = std::move(handle);
			coordination->state = HeldDDLCoordinationState::HELD;
		}
	}
	coordination->ready.notify_all();
	if (failure) {
		std::rethrow_exception(failure);
	}
}

void DuckTransaction::HoldReclusterWriteLock(DataTableInfo &info, bool exclusive) {
	auto &recluster = GetOrCreateReclusterState();
	shared_ptr<HeldTableGate> gate;
	bool try_only = false;
	{
		unique_lock<mutex> guard(recluster.lock);
		auto entry = recluster.table_write_locks.find(info);
		if (entry != recluster.table_write_locks.end()) {
			gate = entry->second;
			while (gate->mode == HeldTableGateMode::ACQUIRING_SHARED ||
			       gate->mode == HeldTableGateMode::ACQUIRING_EXCLUSIVE) {
				gate->ready.wait(guard);
			}
			if (gate->mode == HeldTableGateMode::FAILED) {
				std::rethrow_exception(gate->failure);
			}
			if (exclusive && gate->mode == HeldTableGateMode::SHARED) {
				throw TransactionException("Transaction conflict: cannot acquire an exclusive sorted-table write gate "
				                           "after writing to the table");
			}
			return;
		}

		for (auto &held_entry : recluster.table_write_locks) {
			auto mode = held_entry.second->mode;
			if (exclusive && mode == HeldTableGateMode::EXCLUSIVE) {
				try_only = true;
				continue;
			}
			if (exclusive || mode == HeldTableGateMode::ACQUIRING_EXCLUSIVE || mode == HeldTableGateMode::EXCLUSIVE) {
				throw TransactionException("Transaction conflict: cannot acquire sorted-table write gates for multiple "
				                           "tables around exclusive DDL");
			}
		}

		gate = make_shared_ptr<HeldTableGate>();
		gate->mode = exclusive ? HeldTableGateMode::ACQUIRING_EXCLUSIVE : HeldTableGateMode::ACQUIRING_SHARED;
		recluster.table_write_locks.emplace(std::ref(info), gate);
	}

	unique_ptr<StorageLockKey> handle;
	std::exception_ptr failure;
	try {
		if (exclusive) {
			handle = try_only ? info.TryGetExclusiveReclusterWriteLock() : info.GetExclusiveReclusterWriteLock();
			if (!handle) {
				throw TransactionException("Transaction conflict: cannot immediately acquire an exclusive sorted-table "
				                           "write gate for another table");
			}
		} else {
			handle = info.GetSharedReclusterWriteLock();
		}
	} catch (...) {
		failure = std::current_exception();
	}

	{
		lock_guard<mutex> guard(recluster.lock);
		if (failure) {
			gate->failure = failure;
			gate->mode = HeldTableGateMode::FAILED;
		} else {
			gate->handle = std::move(handle);
			gate->mode = exclusive ? HeldTableGateMode::EXCLUSIVE : HeldTableGateMode::SHARED;
		}
	}
	gate->ready.notify_all();
	if (failure) {
		std::rethrow_exception(failure);
	}
}

bool DuckTransaction::HoldsReclusterWriteLock(DataTableInfo &info) {
	auto recluster = GetReclusterState();
	if (!recluster) {
		return false;
	}
	lock_guard<mutex> guard(recluster->lock);
	auto entry = recluster->table_write_locks.find(info);
	if (entry == recluster->table_write_locks.end()) {
		return false;
	}
	return entry->second->mode == HeldTableGateMode::SHARED || entry->second->mode == HeldTableGateMode::EXCLUSIVE;
}

vector<QualifiedName> DuckTransaction::GetModifiedReclusterTables(bool include_without_checkpoint) noexcept {
	vector<QualifiedName> result;
	try {
		auto recluster = GetReclusterState();
		if (!recluster) {
			return result;
		}
		lock_guard<mutex> guard(recluster->lock);
		for (auto &entry : recluster->table_write_locks) {
			auto mode = entry.second->mode;
			if ((mode != HeldTableGateMode::SHARED && mode != HeldTableGateMode::EXCLUSIVE) ||
			    !entry.first.get().HasSortStorage()) {
				continue;
			}
			auto &info = entry.first.get();
			if (!include_without_checkpoint) {
				auto state = info.GetReclusterState();
				if (!state) {
					continue;
				}
				if (!state->HasUsableCheckpoint()) {
					continue;
				}
			}
			auto schema_path = info.GetSchemaPath();
			schema_path.insert(schema_path.begin(), info.GetDB().GetName());
			result.emplace_back(std::move(schema_path), info.GetTableName());
		}
	} catch (...) { // NOLINT: background scheduling cannot make a durable commit fail
		result.clear();
	}
	return result;
}

void DuckTransaction::ReleaseReclusterWriteLocks() noexcept {
	auto recluster = GetReclusterState();
	if (!recluster) {
		return;
	}
	lock_guard<mutex> guard(recluster->lock);
	recluster->table_write_locks.clear();
	recluster->ddl_coordination_locks.clear();
}

} // namespace duckdb
