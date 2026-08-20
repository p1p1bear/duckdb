//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_delete_journal.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

#include <atomic>

namespace duckdb {

enum class DeleteSlotState : uint8_t { RESERVED, COMMITTED, ABORTED };

struct ReclusterDeleteJournalLimits {
	idx_t max_slots = 0;
	idx_t max_rowids = 0;
};

class ReclusterDeleteJournal;

class ReclusterDeleteSlot {
public:
	delete_sequence_t GetSequence() const {
		return sequence;
	}
	DeleteSlotState GetState() const {
		return state.load(std::memory_order_acquire);
	}
	const vector<row_t> &GetOldRowIds() const {
		return old_rowids;
	}

private:
	friend class ReclusterDeleteJournal;

	ReclusterDeleteSlot(ReclusterDeleteJournal &owner, delete_sequence_t sequence, vector<row_t> old_rowids);
	bool Resolve(DeleteSlotState target) noexcept;

private:
	ReclusterDeleteJournal &owner;
	delete_sequence_t sequence;
	std::atomic<DeleteSlotState> state;
	vector<row_t> old_rowids;
};

struct ReclusterDeleteJournalScan {
	vector<reference<const ReclusterDeleteSlot>> slots;
	delete_sequence_t resolved_through = 0;
	idx_t committed_rowid_count = 0;
	bool blocked_by_reserved = false;
	bool limit_exceeded = false;
};

class ReclusterDeleteJournal {
public:
	explicit ReclusterDeleteJournal(ReclusterDeleteJournalLimits limits);

	optional_ptr<ReclusterDeleteSlot> TryReserve(vector<row_t> old_rowids) noexcept;
	bool Resolve(ReclusterDeleteSlot &slot, DeleteSlotState target) noexcept;
	ReclusterDeleteJournalScan ScanResolved(delete_sequence_t after_sequence, idx_t max_slots, idx_t max_rowids) const;

	delete_sequence_t GetLatestSequence() const;
	idx_t GetSlotCount() const;
	idx_t GetRowIdCount() const;
	const ReclusterDeleteJournalLimits &GetLimits() const {
		return limits;
	}

private:
	ReclusterDeleteJournalLimits limits;
	mutable mutex lock;
	vector<unique_ptr<ReclusterDeleteSlot>> slots;
	idx_t rowid_count = 0;
};

} // namespace duckdb
