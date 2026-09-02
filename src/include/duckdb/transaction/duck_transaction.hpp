//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/enums/active_transaction_state.hpp"

namespace duckdb {
class CheckpointLock;
class CommitDropState;
class DuckTableEntry;
class RowGroupCollection;
class RowVersionManager;
class ReclusterCommitInfo;
class DuckTransactionManager;
class StorageLockKey;
class StorageCommitState;
struct QualifiedName;
struct DataTableInfo;
struct DuckTransactionReclusterState;
struct UndoBufferProperties;

struct CommitInfo {
	transaction_t commit_id;
	ActiveTransactionState active_transactions = ActiveTransactionState::UNSET;
	optional_ptr<CommitDropState> drop_state;
};

class DuckTransaction : public Transaction {
public:
	DuckTransaction(DuckTransactionManager &manager, ClientContext &context, transaction_t start_time,
	                transaction_t transaction_id, idx_t catalog_version);
	~DuckTransaction() override;

	//! The start timestamp of this transaction
	transaction_t start_time;
	//! The transaction id of this transaction
	transaction_t transaction_id;
	//! The commit id of this transaction, if it has successfully been committed
	transaction_t commit_id;

	atomic<idx_t> catalog_version;

	//! Transactions undergo Cleanup, after (1) removing them directly in RemoveTransaction,
	//! or (2) after they enter cleanup_queue.
	//! Some (after rollback) enter cleanup_queue, but do not require Cleanup.
	bool awaiting_cleanup;

public:
	static DuckTransaction &Get(ClientContext &context, AttachedDatabase &db);
	static DuckTransaction &Get(ClientContext &context, Catalog &catalog);
	LocalStorage &GetLocalStorage();

	void PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size);
	void PushAttach(AttachedDatabase &db);

	void SetModifications(DatabaseModificationType type) override;

	bool ShouldWriteToWAL(AttachedDatabase &db);
	ErrorData WriteToWAL(ClientContext &context, AttachedDatabase &db,
	                     unique_ptr<StorageCommitState> &commit_state) noexcept;
	//! Commit the current transaction with the given commit identifier. Returns an error message if the transaction
	//! commit failed, or an empty string if the commit was successful
	ErrorData Commit(AttachedDatabase &db, CommitInfo &commit_info,
	                 unique_ptr<StorageCommitState> commit_state) noexcept;
	bool CommitFinalizationIrreversible() const noexcept {
		return commit_finalization_irreversible;
	}
	//! Returns whether or not a commit of this transaction should trigger an automatic checkpoint
	bool AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties);

	//! Rollback
	ErrorData Rollback();
	//! Cleanup the undo buffer
	void Cleanup(transaction_t lowest_active_transaction);

	bool ChangesMade();
	UndoBufferProperties GetUndoProperties();

	void PushDelete(DuckTableEntry &table_entry, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
	                idx_t base_row);
	void RecordReclusterDeletes(DataTableInfo &info, row_t vector_base, const row_t rows[], idx_t count) noexcept;
	ErrorData PrepareReclusterCommit() noexcept;
	void ResolveReclusterDeletes(bool committed) noexcept;
	void PushSequenceUsage(SequenceCatalogEntry &entry, const SequenceData &data);
	void PushAppend(DuckTableEntry &table_entry, idx_t row_start, idx_t row_count);
	UndoBufferReference CreateUpdateInfo(DuckTableEntry &table_entry, idx_t type_size, idx_t entries,
	                                     idx_t row_group_start);

	DuckTransactionManager &GetTransactionManager();
	bool IsDuckTransaction() const override {
		return true;
	}

	unique_ptr<StorageLockKey> TryGetCheckpointLock();

	//! Get a shared lock on a table
	shared_ptr<CheckpointLock> SharedLockTable(DataTableInfo &info);
	//! Hold a sorted-table write gate until this transaction ends.
	void HoldSharedReclusterWriteLock(DataTableInfo &info);
	void HoldExclusiveReclusterWriteLock(DataTableInfo &info);
	void HoldReclusterDDLCoordinationLock(DataTableInfo &info);
	bool HoldsReclusterWriteLock(DataTableInfo &info);
	vector<QualifiedName> GetModifiedReclusterTables(bool include_without_checkpoint) noexcept;
	void ReleaseReclusterWriteLocks() noexcept;

	void SetIsCheckpointTransaction() {
		is_checkpoint_transaction = true;
		SetIsReclusterMaintenanceTransaction();
	}
	void SetIsReclusterMaintenanceTransaction();
	void PushRecluster(unique_ptr<ReclusterCommitInfo> info);

private:
	void HoldReclusterWriteLock(DataTableInfo &info, bool exclusive);
	DuckTransactionReclusterState &GetOrCreateReclusterState();
	optional_ptr<DuckTransactionReclusterState> GetReclusterState();

	//! The undo buffer is used to store old versions of rows that are updated
	//! or deleted
	UndoBuffer undo_buffer;
	//! The set of uncommitted appends for the transaction
	unique_ptr<LocalStorage> storage;
	//! Lock that prevents checkpoints from starting
	unique_ptr<StorageLockKey> checkpoint_lock;
	//! Lock that prevents vacuums from starting
	unique_ptr<StorageLockKey> vacuum_lock;
	//! Lock for accessing sequence_usage
	mutex sequence_lock;
	//! Map of all sequences that were used during the transaction and the value they had in this transaction
	reference_map_t<SequenceCatalogEntry, reference<SequenceValue>> sequence_usage;
	//! Lock for the active_locks map
	mutex active_locks_lock;
	struct ActiveTableLock {
		mutex checkpoint_lock_mutex; // protects access to the checkpoint_lock field in this class
		weak_ptr<CheckpointLock> checkpoint_lock;
	};
	//! Active locks on tables
	reference_map_t<DataTableInfo, unique_ptr<ActiveTableLock>> active_locks;
	//! State used only by transactions that touch sorted tables or publish recluster work.
	unique_ptr<DuckTransactionReclusterState> recluster_state;
	//! A durable commit reached non-revertible storage finalization before reporting an error.
	bool commit_finalization_irreversible = false;
	//! Flag to prevent auto-checkpointing inside a checkpoint transaction.
	bool is_checkpoint_transaction = false;
};

} // namespace duckdb
