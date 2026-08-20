#include "duckdb/storage/recluster/recluster_range_scanner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/query_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

ReclusterRangeScanner::ReclusterRangeScanner(ReclusterTaskContext &task_context_p)
    : ReclusterRangeScanner(task_context_p, task_context_p.GetCandidate().range) {
}

ReclusterRangeScanner::ReclusterRangeScanner(ReclusterTaskContext &task_context_p, RowGroupRange range)
    : task_context(task_context_p), output_types(GetOutputTypes(task_context_p)),
      scan_state(make_uniq<TableScanState>()) {
	if (!task_context.HasActiveSnapshot()) {
		throw InternalException("Cannot initialize a recluster scan without an active read snapshot");
	}
	auto &candidate_range = task_context.GetCandidate().range;
	if (range.start < candidate_range.start || range.end > candidate_range.end || range.start >= range.end) {
		throw InternalException("Recluster scan range is outside the reserved candidate range");
	}

	vector<StorageIndex> column_ids;
	auto &storage = *task_context.GetStorage();
	column_ids.reserve(storage.Columns().size() + 1);
	for (idx_t column_index = 0; column_index < storage.Columns().size(); column_index++) {
		column_ids.emplace_back(column_index);
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);

	scan_state->Initialize(column_ids);
	storage.GetRowGroupCollection()->InitializeScanWithOffset(
	    TransactionData(task_context.GetSnapshotTransaction()), QueryContext(), scan_state->table_state, column_ids,
	    NumericCast<idx_t>(range.start), NumericCast<idx_t>(range.end));
}

ReclusterRangeScanner::~ReclusterRangeScanner() {
}

vector<LogicalType> ReclusterRangeScanner::GetOutputTypes(const ReclusterTaskContext &task_context) {
	vector<LogicalType> result;
	auto &columns = task_context.GetStorage()->Columns();
	result.reserve(columns.size() + 1);
	for (auto &column : columns) {
		result.push_back(column.Type());
	}
	result.push_back(LogicalType::BIGINT);
	return result;
}

void ReclusterRangeScanner::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(Allocator::Get(task_context.GetSnapshotContext()), output_types);
}

bool ReclusterRangeScanner::Scan(DataChunk &chunk) {
	if (finished) {
		chunk.Reset();
		return false;
	}
	if (chunk.ColumnCount() != output_types.size()) {
		throw InternalException("Recluster scan output chunk has an invalid column count");
	}
	chunk.Reset();
	if (!scan_state->table_state.Scan(task_context.GetSnapshotTransaction(), chunk)) {
		finished = true;
		return false;
	}
	if (chunk.size() == 0 || chunk.size() > NumericLimits<idx_t>::Maximum() - scanned_row_count) {
		throw InternalException("Recluster range scan produced an invalid row count");
	}
	scanned_row_count += chunk.size();
	return true;
}

} // namespace duckdb
