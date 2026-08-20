//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_range_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

class DataChunk;
class ReclusterTaskContext;
class TableScanState;

class ReclusterRangeScanner {
public:
	explicit ReclusterRangeScanner(ReclusterTaskContext &task_context);
	ReclusterRangeScanner(ReclusterTaskContext &task_context, RowGroupRange range);
	~ReclusterRangeScanner();

	static vector<LogicalType> GetOutputTypes(const ReclusterTaskContext &task_context);
	void InitializeChunk(DataChunk &chunk) const;
	bool Scan(DataChunk &chunk);
	const vector<LogicalType> &GetOutputTypes() const {
		return output_types;
	}
	idx_t GetScannedRowCount() const {
		return scanned_row_count;
	}

private:
	ReclusterTaskContext &task_context;
	vector<LogicalType> output_types;
	unique_ptr<TableScanState> scan_state;
	idx_t scanned_row_count = 0;
	bool finished = false;
};

} // namespace duckdb
