#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

RowGroupColumnDropOwnership::RowGroupColumnDropOwnership() {
}

RowGroupColumnDropOwnership::RowGroupColumnDropOwnership(vector<block_id_t> immutable_drop_actions_p)
    : RowGroupColumnDropOwnership() {
	FreezeOrVerify(std::move(immutable_drop_actions_p));
}

bool RowGroupColumnDropOwnership::FreezeOrVerify(vector<block_id_t> immutable_drop_actions_p) {
	for (auto block_id : immutable_drop_actions_p) {
		if (block_id < 0 || block_id >= MAXIMUM_BLOCK) {
			throw InternalException("Invalid block ID %d in row group column drop ownership", block_id);
		}
	}
	if (frozen) {
		if (immutable_drop_actions != immutable_drop_actions_p) {
			throw InternalException("Cannot change frozen row group column drop ownership actions");
		}
		return false;
	}
	immutable_drop_actions = std::move(immutable_drop_actions_p);
	frozen = true;
	return true;
}

const vector<block_id_t> &RowGroupColumnDropOwnership::GetActions() const {
	if (!frozen) {
		throw InternalException("Cannot read unfrozen row group column drop ownership actions");
	}
	return immutable_drop_actions;
}

bool RowGroupColumnDropOwnership::Claim(const shared_ptr<RowGroupColumnDropClaim> &claim_owner_p) {
	if (!claim_owner_p) {
		throw InternalException("Cannot claim row group column drop ownership without an owner");
	}
	if (!frozen) {
		throw InternalException("Cannot claim unfrozen row group column drop ownership");
	}
	switch (state) {
	case State::LIVE:
		state = State::CLAIMED;
		claim_owner = claim_owner_p;
		return true;
	case State::CLAIMED:
		if (claim_owner == claim_owner_p) {
			return false;
		}
		throw InternalException("Row group column drop ownership is claimed by another owner");
	case State::DROPPED:
		return false;
	case State::POISONED:
		throw InternalException("Cannot claim poisoned row group column drop ownership");
	}
	throw InternalException("Unknown row group column drop ownership state");
}

bool RowGroupColumnDropOwnership::TryTransition(const RowGroupColumnDropClaim &claim_owner_p, State target) noexcept {
	if (state != State::CLAIMED || claim_owner.get() != &claim_owner_p) {
		return false;
	}
	state = target;
	claim_owner.reset();
	return true;
}

bool RowGroupColumnDropOwnership::Finalize(const RowGroupColumnDropClaim &claim_owner_p) noexcept {
	return TryTransition(claim_owner_p, State::DROPPED);
}

bool RowGroupColumnDropOwnership::Revert(const RowGroupColumnDropClaim &claim_owner_p) noexcept {
	return TryTransition(claim_owner_p, State::LIVE);
}

bool RowGroupColumnDropOwnership::Poison(const RowGroupColumnDropClaim &claim_owner_p) noexcept {
	return TryTransition(claim_owner_p, State::POISONED);
}

} // namespace duckdb
