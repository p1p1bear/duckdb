//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/row_id_remap_store.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"

namespace duckdb {

static constexpr row_t INVALID_REMAP_ROW_ID = NumericLimits<row_t>::Maximum();
static constexpr uint32_t INVALID_REMAP_ROW_OFFSET = NumericLimits<uint32_t>::Maximum();

struct RowIdRemapChunk {
	row_t old_start;
	vector<uint32_t> new_rowid_offsets;
	vector<row_t> new_rowids;

	idx_t Count() const {
		return new_rowid_offsets.empty() ? new_rowids.size() : new_rowid_offsets.size();
	}
	row_t GetOldEnd() const {
		return old_start + NumericCast<row_t>(Count());
	}
};

class RowIdRemapStore {
public:
	explicit RowIdRemapStore(const vector<RowGroupPhysicalIdentity> &row_groups);

	row_t GetNewRowId(row_t old_rowid) const;
	void SetNewRowId(row_t old_rowid, row_t new_rowid);
	idx_t GetMappedCount() const {
		return mapped_count;
	}
	idx_t GetPhysicalRowCount() const {
		return physical_row_count;
	}
	idx_t GetAllocationSize() const;
	bool UsesCompactOffsets() const {
		return compact_offsets;
	}
	static idx_t GetEntrySize(idx_t physical_row_count);
	static idx_t GetMaxEntries(idx_t byte_budget);
	const vector<RowIdRemapChunk> &GetChunks() const {
		return chunks;
	}

private:
	idx_t GetChunkIndex(row_t old_rowid) const;
	RowIdRemapChunk &GetChunk(row_t old_rowid);
	const RowIdRemapChunk &GetChunk(row_t old_rowid) const;

private:
	vector<RowIdRemapChunk> chunks;
	row_t new_rowid_base = 0;
	idx_t mapped_count = 0;
	idx_t physical_row_count = 0;
	bool compact_offsets = false;
};

} // namespace duckdb
