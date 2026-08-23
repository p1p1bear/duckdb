//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_column_drop_ownership.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class RowGroupColumnDropClaim final {
public:
	RowGroupColumnDropClaim() = default;
	RowGroupColumnDropClaim(const RowGroupColumnDropClaim &) = delete;
	RowGroupColumnDropClaim &operator=(const RowGroupColumnDropClaim &) = delete;
	RowGroupColumnDropClaim(RowGroupColumnDropClaim &&) = delete;
	RowGroupColumnDropClaim &operator=(RowGroupColumnDropClaim &&) = delete;
};

class RowGroupColumnDropOwnership {
public:
	enum class State : uint8_t { LIVE, CLAIMED, DROPPED, POISONED };

	RowGroupColumnDropOwnership();
	explicit RowGroupColumnDropOwnership(vector<block_id_t> immutable_drop_actions);
	RowGroupColumnDropOwnership(const RowGroupColumnDropOwnership &) = delete;
	RowGroupColumnDropOwnership &operator=(const RowGroupColumnDropOwnership &) = delete;
	RowGroupColumnDropOwnership(RowGroupColumnDropOwnership &&) = delete;
	RowGroupColumnDropOwnership &operator=(RowGroupColumnDropOwnership &&) = delete;

public:
	//! Returns true only when this call freezes the actions for the first time.
	bool FreezeOrVerify(vector<block_id_t> immutable_drop_actions);
	//! Returns true only when this call changes LIVE to CLAIMED.
	bool Claim(const shared_ptr<RowGroupColumnDropClaim> &claim_owner);
	//! These return false on an invalid transition; a guard must treat that as an invariant failure.
	bool Finalize(const RowGroupColumnDropClaim &claim_owner) noexcept;
	bool Revert(const RowGroupColumnDropClaim &claim_owner) noexcept;
	bool Poison(const RowGroupColumnDropClaim &claim_owner) noexcept;

	const vector<block_id_t> &GetActions() const;
	bool IsFrozen() const noexcept {
		return frozen;
	}
	State GetState() const noexcept {
		return state;
	}
	bool IsClaimedBy(const RowGroupColumnDropClaim &owner) const noexcept {
		return state == State::CLAIMED && claim_owner.get() == &owner;
	}

private:
	bool TryTransition(const RowGroupColumnDropClaim &claim_owner, State target) noexcept;

private:
	//! Access is serialized by StorageManager's ownership drop lock.
	vector<block_id_t> immutable_drop_actions;
	bool frozen = false;
	State state = State::LIVE;
	shared_ptr<RowGroupColumnDropClaim> claim_owner;
};

} // namespace duckdb
