#include "duckdb/storage/wal_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

void WALReclusterEntry::Validate() const {
	if (table_id == hugeint_t(0, 0) || task_id == hugeint_t(0, 0) || range_start < 0 || range_start >= range_end ||
	    !manifest_pointer.IsValid() || manifest_size == 0 ||
	    expected_layout_version == NumericLimits<layout_version_t>::Maximum() ||
	    target_layout_version != expected_layout_version + 1) {
		throw SerializationException("Invalid recluster WAL header");
	}
	if ((final_delete_row_count == 0) != (delete_chunk_count == 0)) {
		throw SerializationException("Invalid recluster WAL DELETE counts");
	}
	if (final_delete_row_count != 0) {
		auto minimum_chunks = final_delete_row_count / STANDARD_VECTOR_SIZE;
		minimum_chunks += final_delete_row_count % STANDARD_VECTOR_SIZE != 0;
		if (minimum_chunks > NumericLimits<uint32_t>::Maximum() || delete_chunk_count < minimum_chunks ||
		    delete_chunk_count > final_delete_row_count) {
			throw SerializationException("Invalid recluster WAL DELETE counts");
		}
	}
}

void WALReclusterDeleteEntry::Validate() const {
	if (table_id == hugeint_t(0, 0) || task_id == hugeint_t(0, 0) || new_rowids.empty() ||
	    new_rowids.size() > STANDARD_VECTOR_SIZE) {
		throw SerializationException("Invalid recluster WAL DELETE chunk");
	}
	for (auto row_id : new_rowids) {
		if (row_id < 0) {
			throw SerializationException("Invalid row ID in recluster WAL DELETE chunk");
		}
	}
}

} // namespace duckdb
