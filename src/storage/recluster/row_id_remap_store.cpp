#include "duckdb/storage/recluster/row_id_remap_store.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <algorithm>

namespace duckdb {

RowIdRemapStore::RowIdRemapStore(const vector<RowGroupPhysicalIdentity> &row_groups) {
	if (row_groups.empty()) {
		throw InternalException("A recluster row ID remap requires at least one row group");
	}
	optional<row_t> previous_end;
	chunks.reserve(row_groups.size());
	for (auto &row_group : row_groups) {
		if (row_group.start < 0 || row_group.count == 0 ||
		    row_group.count > static_cast<idx_t>(NumericLimits<row_t>::Maximum() - row_group.start)) {
			throw InternalException("Cannot build a row ID remap for an invalid row group range");
		}
		auto row_group_end = row_group.start + NumericCast<row_t>(row_group.count);
		if (previous_end && row_group.start < *previous_end) {
			throw InternalException("Cannot build a row ID remap for overlapping row groups");
		}
		if (row_group.count > NumericLimits<idx_t>::Maximum() - physical_row_count) {
			throw InternalException("Recluster row ID remap size overflow");
		}
		physical_row_count += row_group.count;
		chunks.push_back({row_group.start, vector<row_t>(row_group.count, INVALID_REMAP_ROW_ID)});
		previous_end = row_group_end;
	}
}

idx_t RowIdRemapStore::GetChunkIndex(row_t old_rowid) const {
	auto entry = std::upper_bound(chunks.begin(), chunks.end(), old_rowid,
	                              [](row_t rowid, const RowIdRemapChunk &chunk) { return rowid < chunk.old_start; });
	if (entry == chunks.begin()) {
		throw InternalException("Old row ID %lld is outside the recluster input", old_rowid);
	}
	entry--;
	if (old_rowid >= entry->GetOldEnd()) {
		throw InternalException("Old row ID %lld is outside the recluster input", old_rowid);
	}
	return NumericCast<idx_t>(entry - chunks.begin());
}

const RowIdRemapChunk &RowIdRemapStore::GetChunk(row_t old_rowid) const {
	return chunks[GetChunkIndex(old_rowid)];
}

RowIdRemapChunk &RowIdRemapStore::GetChunk(row_t old_rowid) {
	return chunks[GetChunkIndex(old_rowid)];
}

row_t RowIdRemapStore::GetNewRowId(row_t old_rowid) const {
	auto &chunk = GetChunk(old_rowid);
	return chunk.new_rowids[NumericCast<idx_t>(old_rowid - chunk.old_start)];
}

void RowIdRemapStore::SetNewRowId(row_t old_rowid, row_t new_rowid) {
	if (new_rowid < 0 || new_rowid == INVALID_REMAP_ROW_ID) {
		throw InternalException("Invalid new row ID in recluster remap");
	}
	auto &chunk = GetChunk(old_rowid);
	auto &target = chunk.new_rowids[NumericCast<idx_t>(old_rowid - chunk.old_start)];
	if (target != INVALID_REMAP_ROW_ID) {
		throw InternalException("Old row ID %lld was mapped more than once", old_rowid);
	}
	target = new_rowid;
	mapped_count++;
}

idx_t RowIdRemapStore::GetAllocationSize() const {
	if (physical_row_count > NumericLimits<idx_t>::Maximum() / sizeof(row_t)) {
		throw InternalException("Recluster row ID remap allocation size overflow");
	}
	return physical_row_count * sizeof(row_t);
}

} // namespace duckdb
