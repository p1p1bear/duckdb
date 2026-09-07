//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_task_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/row_id_remap_store.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class Connection;
class DataTable;
class DuckTransaction;
class ReclusterOutput;

class ReclusterTaskContext {
public:
	ReclusterTaskContext(persistent_table_id_t table_id, uint64_t initialization_token, ReclusterCandidate candidate,
	                     SortOrderDefinition sort_definition, vector<idx_t> physical_sort_indexes,
	                     shared_ptr<DataTable> storage, AttachedDatabase &db,
	                     optional_ptr<ClientContext> driver_context = nullptr, idx_t max_threads = 0);
	~ReclusterTaskContext();

	persistent_table_id_t GetTableId() const {
		return table_id;
	}
	uint64_t GetInitializationToken() const {
		return initialization_token;
	}
	const ReclusterCandidate &GetCandidate() const {
		return candidate;
	}
	const SortOrderDefinition &GetSortDefinition() const {
		return sort_definition;
	}
	const vector<idx_t> &GetPhysicalSortIndexes() const {
		return physical_sort_indexes;
	}
	const shared_ptr<DataTable> &GetStorage() const {
		return storage;
	}
	RowIdRemapStore &GetRowIdRemap() {
		return row_id_remap;
	}
	const RowIdRemapStore &GetRowIdRemap() const {
		return row_id_remap;
	}
	transaction_t GetSnapshotStartTime() const {
		return snapshot_start_time;
	}
	bool HasActiveSnapshot() const;
	ClientContext &GetSnapshotContext();
	DuckTransaction &GetSnapshotTransaction();
	void InterruptCheck() const;
	idx_t GetThreadLimit(idx_t available_threads) const;
	void CloseSnapshot();
	bool HasOutput() const {
		return output != nullptr;
	}
	ReclusterOutput &GetOutput();
	const ReclusterOutput &GetOutput() const;
	void SetOutput(unique_ptr<ReclusterOutput> output);

private:
	persistent_table_id_t table_id;
	uint64_t initialization_token;
	ReclusterCandidate candidate;
	SortOrderDefinition sort_definition;
	vector<idx_t> physical_sort_indexes;
	shared_ptr<DataTable> storage;
	RowIdRemapStore row_id_remap;
	shared_ptr<AttachedDatabase> db;
	unique_ptr<Connection> snapshot_connection;
	optional_ptr<ClientContext> driver_context;
	idx_t max_threads;
	transaction_t snapshot_start_time;
	unique_ptr<ReclusterOutput> output;
};

} // namespace duckdb
