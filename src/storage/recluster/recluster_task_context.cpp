#include "duckdb/storage/recluster/recluster_task_context.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

ReclusterTaskContext::ReclusterTaskContext(persistent_table_id_t table_id_p, uint64_t initialization_token_p,
                                           ReclusterCandidate candidate_p, SortOrderDefinition sort_definition_p,
                                           vector<idx_t> physical_sort_indexes_p, shared_ptr<DataTable> storage_p,
                                           AttachedDatabase &db_p)
    : table_id(table_id_p), initialization_token(initialization_token_p), candidate(std::move(candidate_p)),
      sort_definition(std::move(sort_definition_p)), physical_sort_indexes(std::move(physical_sort_indexes_p)),
      storage(std::move(storage_p)), db(db_p.shared_from_this()), snapshot_start_time(0) {
	if (table_id == hugeint_t(0, 0) || initialization_token == 0 || !storage || sort_definition.columns.empty() ||
	    sort_definition.sort_order_id != candidate.sort_order_id ||
	    physical_sort_indexes.size() != sort_definition.columns.size()) {
		throw InternalException("Invalid recluster task context");
	}

	snapshot_connection = make_uniq<Connection>(db_p.GetDatabase());
	auto &transaction_context = snapshot_connection->context->transaction;
	transaction_context.BeginTransaction();
	transaction_context.SetReadOnly();
	auto &transaction = DuckTransaction::Get(*snapshot_connection->context, db_p);
	snapshot_start_time = transaction.start_time;
}

ReclusterTaskContext::~ReclusterTaskContext() {
}

bool ReclusterTaskContext::HasActiveSnapshot() const {
	return snapshot_connection && snapshot_connection->context->transaction.HasActiveTransaction();
}

ClientContext &ReclusterTaskContext::GetSnapshotContext() {
	if (!HasActiveSnapshot()) {
		throw InternalException("Recluster task read snapshot is no longer active");
	}
	return *snapshot_connection->context;
}

DuckTransaction &ReclusterTaskContext::GetSnapshotTransaction() {
	return DuckTransaction::Get(GetSnapshotContext(), *db);
}

void ReclusterTaskContext::CloseSnapshot() {
	if (!snapshot_connection) {
		return;
	}
	auto connection = std::move(snapshot_connection);
	if (connection->context->transaction.HasActiveTransaction()) {
		connection->context->transaction.Rollback(nullptr);
	}
}

} // namespace duckdb
