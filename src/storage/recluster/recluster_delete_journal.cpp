#include "duckdb/storage/recluster/recluster_delete_journal.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

ReclusterDeleteSlot::ReclusterDeleteSlot(ReclusterDeleteJournal &owner_p, delete_sequence_t sequence_p,
                                         vector<row_t> old_rowids_p)
    : owner(owner_p), sequence(sequence_p), state(DeleteSlotState::RESERVED), old_rowids(std::move(old_rowids_p)) {
}

bool ReclusterDeleteSlot::Resolve(DeleteSlotState target) noexcept {
	if (target == DeleteSlotState::RESERVED) {
		return false;
	}
	auto expected = DeleteSlotState::RESERVED;
	if (state.compare_exchange_strong(expected, target, std::memory_order_acq_rel, std::memory_order_acquire)) {
		return true;
	}
	return expected == target;
}

ReclusterDeleteJournal::ReclusterDeleteJournal(ReclusterDeleteJournalLimits limits_p) : limits(limits_p) {
	if (limits.max_slots == 0 || limits.max_rowids == 0) {
		throw InternalException("A recluster DELETE journal requires non-zero capacity limits");
	}
}

optional_ptr<ReclusterDeleteSlot> ReclusterDeleteJournal::TryReserve(vector<row_t> old_rowids) noexcept {
	if (old_rowids.empty()) {
		return nullptr;
	}

	lock_guard<mutex> guard(lock);
	if (slots.size() >= limits.max_slots || old_rowids.size() > limits.max_rowids - rowid_count ||
	    slots.size() >= NumericLimits<delete_sequence_t>::Maximum() - 1) {
		return nullptr;
	}

	try {
		auto slot_rowid_count = old_rowids.size();
		auto sequence = NumericCast<delete_sequence_t>(slots.size() + 1);
		auto slot = unique_ptr<ReclusterDeleteSlot>(new ReclusterDeleteSlot(*this, sequence, std::move(old_rowids)));
		slots.push_back(std::move(slot));
		rowid_count += slot_rowid_count;
		return *slots.back();
	} catch (...) {
		return nullptr;
	}
}

bool ReclusterDeleteJournal::Resolve(ReclusterDeleteSlot &slot, DeleteSlotState target) noexcept {
	if (&slot.owner != this) {
		return false;
	}
	return slot.Resolve(target);
}

ReclusterDeleteJournalScan ReclusterDeleteJournal::ScanResolved(delete_sequence_t after_sequence, idx_t max_slots,
                                                                idx_t max_rowids) const {
	lock_guard<mutex> guard(lock);
	if (after_sequence > slots.size()) {
		throw InternalException("Recluster DELETE journal scan starts after its latest sequence");
	}

	ReclusterDeleteJournalScan result;
	result.resolved_through = after_sequence;
	for (idx_t slot_index = NumericCast<idx_t>(after_sequence); slot_index < slots.size(); slot_index++) {
		auto &slot = *slots[slot_index];
		auto state = slot.GetState();
		if (state == DeleteSlotState::RESERVED) {
			result.blocked_by_reserved = true;
			break;
		}
		if (result.slots.size() >= max_slots) {
			result.limit_exceeded = true;
			break;
		}
		if (state == DeleteSlotState::COMMITTED &&
		    slot.GetOldRowIds().size() > max_rowids - result.committed_rowid_count) {
			result.limit_exceeded = true;
			break;
		}

		result.slots.emplace_back(slot);
		result.resolved_through = slot.GetSequence();
		if (state == DeleteSlotState::COMMITTED) {
			result.committed_rowid_count += slot.GetOldRowIds().size();
		}
	}
	return result;
}

delete_sequence_t ReclusterDeleteJournal::GetLatestSequence() const {
	lock_guard<mutex> guard(lock);
	return NumericCast<delete_sequence_t>(slots.size());
}

idx_t ReclusterDeleteJournal::GetSlotCount() const {
	lock_guard<mutex> guard(lock);
	return slots.size();
}

idx_t ReclusterDeleteJournal::GetRowIdCount() const {
	lock_guard<mutex> guard(lock);
	return rowid_count;
}

idx_t ReclusterDeleteJournal::GetCommittedRowIdCountAfter(delete_sequence_t sequence) const {
	lock_guard<mutex> guard(lock);
	if (sequence > slots.size()) {
		throw InternalException("Recluster DELETE journal status starts after its latest sequence");
	}
	idx_t result = 0;
	for (idx_t slot_index = NumericCast<idx_t>(sequence); slot_index < slots.size(); slot_index++) {
		auto &slot = *slots[slot_index];
		if (slot.GetState() == DeleteSlotState::COMMITTED) {
			result += slot.GetOldRowIds().size();
		}
	}
	return result;
}

} // namespace duckdb
