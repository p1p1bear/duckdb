//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/undo_buffer.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/enums/index_removal_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
class BlockManager;
class CatalogEntry;
class TableIndexList;
class DataChunk;
class DuckTransaction;
class ReclusterCommitInfo;
class RowGroupColumnDropOwnership;
class WriteAheadLog;
class ClientContext;
class CommitDropStatePrepared;

struct DataTableInfo;
class DataTable;
struct DeleteInfo;
struct UpdateInfo;

enum class CommitMode { COMMIT, REVERT_COMMIT };

//! An index that has been marked for removal from a table's index list once the commit chain succeeds.
struct PendingIndexRemoval {
	reference<TableIndexList> indexes;
	Identifier name;
};

//! Accumulates block marks and index removals during commit so they can be applied together once the
//! commit chain has succeeded and FlushCommit() has been called, since these are side effects that can't be reverted
//! if we need to rollback a transaction.
class CommitDropState {
public:
	explicit CommitDropState(optional_ptr<BlockManager> block_manager);
	~CommitDropState();

public:
	//! Register one concrete column node's persistent ownership for commit-time release.
	void DropColumnOwnership(shared_ptr<RowGroupColumnDropOwnership> ownership, vector<block_id_t> actions);
	//! Register an index to be removed from a table's index list during FinalizeCommit. Index removal will drop in
	//! memory index data and also marks all blocks on disk as free blocks allowing for reclamation. Block marking for
	//! indexes is handled implicitly along destruction paths for index memory.
	void RemoveIndex(TableIndexList &indexes, Identifier name);
	//! Register a recluster ownership transfer to run only after the WAL is durable.
	void AddRecluster(ReclusterCommitInfo &info);
	//! Claim and validate all registered block drops before the WAL commit is flushed.
	void PrepareFinalize();
	//! Release a prepared claim before reverting a failed commit.
	void RevertPrepared() noexcept;
	//! Finalize accumulated block marks and index removals.
	void FinalizeCommit();
	//! True once FinalizeCommit has begun applying non-revertible side effects.
	bool IrreversibleFinalizationStarted() const noexcept {
		return irreversible_finalization_started;
	}
	//! True if no work has been queued.
	bool Empty() const;

private:
	struct PendingColumnDropOwnership {
		shared_ptr<RowGroupColumnDropOwnership> ownership;
		vector<block_id_t> actions;
	};

	optional_ptr<BlockManager> block_manager;
	vector<PendingColumnDropOwnership> pending_column_drops;
	vector<PendingIndexRemoval> pending_index_removals;
	optional_ptr<ReclusterCommitInfo> pending_recluster;
	unique_ptr<CommitDropStatePrepared> prepared;
	bool prepare_complete = false;
	bool irreversible_finalization_started = false;
};

struct IndexDataRemover {
public:
	explicit IndexDataRemover(DuckTransaction &transaction, QueryContext context, IndexRemovalType removal_type);

	void PushDelete(DeleteInfo &info);
	void Verify();

private:
	void Flush(DataTable &table, row_t *row_numbers, idx_t count);

private:
	DuckTransaction &transaction;
	// data for index cleanup
	QueryContext context;
	//! While committing, we remove data from any indexes that was deleted
	IndexRemovalType removal_type;
	DataChunk chunk;
	//! Debug mode only - list of indexes to verify
	reference_map_t<DataTable, shared_ptr<DataTableInfo>> verify_indexes;
};

class CommitState {
public:
	explicit CommitState(DuckTransaction &transaction, transaction_t commit_id,
	                     ActiveTransactionState transaction_state, CommitMode commit_mode);

public:
	void CommitEntry(UndoFlags type, data_ptr_t data, CommitInfo &info);
	void RevertCommit(UndoFlags type, data_ptr_t data);
	void Flush();
	void Verify();
	static IndexRemovalType GetIndexRemovalType(ActiveTransactionState transaction_state, CommitMode commit_mode);

private:
	void CommitEntryDrop(CatalogEntry &entry, data_ptr_t extra_data, CommitInfo &info);
	void CommitDelete(DeleteInfo &info);

private:
	DuckTransaction &transaction;
	transaction_t commit_id;
	IndexDataRemover index_data_remover;
};

} // namespace duckdb
