//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_delete_catchup.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

class RangeTask;

struct ReclusterDeleteCatchupResult {
	delete_sequence_t applied_through = 0;
	idx_t resolved_slot_count = 0;
	idx_t mapped_rowid_count = 0;
	idx_t deleted_row_count = 0;
	bool blocked_by_reserved = false;
	bool limit_exceeded = false;
};

class ReclusterDeleteCatchup {
public:
	explicit ReclusterDeleteCatchup(RangeTask &task);

	ReclusterDeleteCatchupResult Run(idx_t max_slots = 0, idx_t max_rowids = 0);

private:
	void CheckTask() const;

private:
	RangeTask &task;
};

} // namespace duckdb
