//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class AttachedDatabase;
class DatabaseInstance;
struct CheckpointOptions;
class TableIOManager;
class RowGroupCollection;
class TableReclusterState;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, vector<Identifier> schema_path,
	              Identifier table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all, if nullptr.
	void BindIndexes(ClientContext &context, const char *index_type = nullptr);

	//! Whether or not the table is temporary
	bool IsTemporary() const;

	AttachedDatabase &GetDB() const {
		return db;
	}

	TableIOManager &GetIOManager() {
		return *table_io_manager;
	}

	TableIndexList &GetIndexes() {
		return indexes;
	}
	//! Find and move out an IndexStorageInfo by name from the stored collection.
	IndexStorageInfo ExtractIndexStorageInfo(const Identifier &name);
	unique_ptr<StorageLockKey> GetSharedLock() {
		return checkpoint_lock.GetSharedLock();
	}
	unique_ptr<StorageLockKey> GetSharedReclusterWriteLock() {
		return recluster_write_gate.GetSharedLock();
	}
	unique_ptr<StorageLockKey> GetExclusiveReclusterWriteLock() {
		return recluster_write_gate.GetExclusiveLock();
	}
	unique_ptr<StorageLockKey> TryGetExclusiveReclusterWriteLock() {
		return recluster_write_gate.TryGetExclusiveLock();
	}
	unique_ptr<StorageLockKey> GetReclusterDDLCoordinationLock() {
		return recluster_ddl_gate.GetExclusiveLock();
	}
	unique_ptr<StorageLockKey> TryGetReclusterDDLCoordinationLock() {
		return recluster_ddl_gate.TryGetExclusiveLock();
	}
	bool AppendRequiresNewRowGroup(RowGroupCollection &collection, transaction_t checkpoint_id);
	optional_idx CheckpointRowGroupCount(const CheckpointOptions &options) const;
	void VerifyIndexBuffers();
	void InitializeSortStorage(const PersistentTableSortStorageMetadata &metadata);
	void ResetSortStorage();
	bool HasSortStorage() const;
	TableSortStorageState &GetSortStorage();
	const TableSortStorageState &GetSortStorage() const;
	shared_ptr<TableReclusterState> GetOrCreateReclusterState(uint64_t initialization_token);
	shared_ptr<TableReclusterState> GetReclusterState() const;

	Identifier GetSchemaName();
	//! The full (possibly nested) schema path of the table
	const vector<Identifier> &GetSchemaPath() const;
	Identifier GetTableName();
	void SetTableName(Identifier name);

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The (possibly nested) schema path of the table, outermost schema first
	vector<Identifier> schema_path;
	//! The name of the table
	Identifier table;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! Lock held while checkpointing
	StorageLock checkpoint_lock;
	//! Lock coordinating sorted-table writers and layout or catalog publication
	StorageLock recluster_write_gate;
	//! Lock serializing sorted-table DDL coordination on this table
	StorageLock recluster_ddl_gate;
	//! The last seen checkpoint while doing a concurrent operation, if any
	optional_idx last_seen_checkpoint;
	//! The amount of row groups the checkpoint is processing
	optional_idx checkpoint_row_group_count;
	//! Physical counters for tables that have SORTED BY history
	unique_ptr<TableSortStorageState> sort_storage;
	atomic<bool> sort_storage_initialized = false;
	//! Optional runtime state for tables that have SORTED BY history
	mutable mutex recluster_state_lock;
	shared_ptr<TableReclusterState> recluster_state;
};

} // namespace duckdb
