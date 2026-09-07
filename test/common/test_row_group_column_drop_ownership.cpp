#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"

#include <type_traits>
#include <utility>

using namespace duckdb; // NOLINT

using DropOwnership = RowGroupColumnDropOwnership;
using DropClaim = RowGroupColumnDropClaim;
using DropActions = duckdb::vector<block_id_t>;

static_assert(!std::is_copy_constructible<DropOwnership>::value, "drop ownership must be shared, not copied");
static_assert(!std::is_move_constructible<DropOwnership>::value, "drop ownership must be shared, not moved");
static_assert(!std::is_copy_constructible<DropClaim>::value, "drop claims must have stable identity");
static_assert(!std::is_move_constructible<DropClaim>::value, "drop claims must have stable identity");
static_assert(noexcept(std::declval<DropOwnership &>().Finalize(std::declval<DropClaim &>())),
              "Finalize must be noexcept");
static_assert(noexcept(std::declval<DropOwnership &>().Revert(std::declval<DropClaim &>())), "Revert must be noexcept");
static_assert(noexcept(std::declval<DropOwnership &>().Poison(std::declval<DropClaim &>())), "Poison must be noexcept");

TEST_CASE("Row group column drop ownership preserves immutable actions", "[storage][drop_ownership]") {
	DropActions actions {7, 3, 7, 5};
	auto ownership = make_shared_ptr<DropOwnership>(actions);

	REQUIRE(ownership->IsFrozen());
	REQUIRE(ownership->GetActions() == actions);
	REQUIRE(ownership->GetState() == DropOwnership::State::LIVE);
}

TEST_CASE("Row group column drop ownership freezes actions once", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>();
	auto owner = make_shared_ptr<DropClaim>();
	DropActions actions {19, 23, 19};

	REQUIRE(!ownership->IsFrozen());
	REQUIRE_THROWS_AS(ownership->GetActions(), InternalException);
	REQUIRE_THROWS_AS(ownership->Claim(owner), InternalException);
	REQUIRE(ownership->GetState() == DropOwnership::State::LIVE);

	REQUIRE(ownership->FreezeOrVerify(actions));
	REQUIRE(ownership->IsFrozen());
	REQUIRE(ownership->GetActions() == actions);
	REQUIRE(!ownership->FreezeOrVerify(actions));
	REQUIRE_THROWS_AS(ownership->FreezeOrVerify(DropActions {19, 19, 23}), InternalException);
	REQUIRE(ownership->GetActions() == actions);
}

TEST_CASE("Row group column drop ownership rejects invalid actions", "[storage][drop_ownership]") {
	REQUIRE_THROWS_AS(make_shared_ptr<DropOwnership>(DropActions {INVALID_BLOCK}), InternalException);
	REQUIRE_THROWS_AS(make_shared_ptr<DropOwnership>(DropActions {MAXIMUM_BLOCK}), InternalException);
	REQUIRE_NOTHROW(make_shared_ptr<DropOwnership>(DropActions {0, MAXIMUM_BLOCK - 1}));

	auto ownership = make_shared_ptr<DropOwnership>();
	REQUIRE_THROWS_AS(ownership->FreezeOrVerify(DropActions {INVALID_BLOCK}), InternalException);
	REQUIRE(!ownership->IsFrozen());
}

TEST_CASE("Row group column drop ownership claims and finalizes once", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>(DropActions {11, 11});
	auto owner = make_shared_ptr<DropClaim>();
	auto other_owner = make_shared_ptr<DropClaim>();
	duckdb::shared_ptr<DropClaim> missing_owner;

	REQUIRE_THROWS_AS(ownership->Claim(missing_owner), InternalException);
	REQUIRE(ownership->GetState() == DropOwnership::State::LIVE);
	REQUIRE(ownership->Claim(owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::CLAIMED);
	REQUIRE(ownership->IsClaimedBy(*owner));

	REQUIRE(!ownership->Claim(owner));
	REQUIRE_THROWS_AS(ownership->Claim(other_owner), InternalException);
	REQUIRE(!ownership->Finalize(*other_owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::CLAIMED);
	REQUIRE(ownership->IsClaimedBy(*owner));

	REQUIRE(ownership->Finalize(*owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::DROPPED);
	REQUIRE(!ownership->Claim(other_owner));
	REQUIRE(!ownership->Finalize(*owner));
	REQUIRE(!ownership->Revert(*owner));
	REQUIRE(!ownership->Poison(*owner));
}

TEST_CASE("Row group column drop ownership can revert a pre-apply claim", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>(DropActions {13});
	auto owner = make_shared_ptr<DropClaim>();
	auto next_owner = make_shared_ptr<DropClaim>();

	REQUIRE(ownership->Claim(owner));
	REQUIRE(!ownership->Revert(*next_owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::CLAIMED);
	REQUIRE(ownership->Revert(*owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::LIVE);

	REQUIRE(ownership->Claim(next_owner));
	REQUIRE(ownership->IsClaimedBy(*next_owner));
	REQUIRE(ownership->Finalize(*next_owner));
}

TEST_CASE("Row group column drop ownership poisons only its claimant", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>(DropActions {17});
	auto owner = make_shared_ptr<DropClaim>();
	auto other_owner = make_shared_ptr<DropClaim>();

	REQUIRE(ownership->Claim(owner));
	REQUIRE(!ownership->Poison(*other_owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::CLAIMED);
	REQUIRE(ownership->Poison(*owner));
	REQUIRE(ownership->GetState() == DropOwnership::State::POISONED);

	REQUIRE_THROWS_AS(ownership->Claim(owner), InternalException);
	REQUIRE(!ownership->Finalize(*owner));
	REQUIRE(!ownership->Revert(*owner));
	REQUIRE(!ownership->Poison(*owner));
}

TEST_CASE("Row group column drop ownership permits empty direct actions", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>(DropActions {});
	auto owner = make_shared_ptr<DropClaim>();

	REQUIRE(ownership->GetActions().empty());
	REQUIRE(ownership->Claim(owner));
	REQUIRE(ownership->Finalize(*owner));
}

TEST_CASE("Row group column drop ownership retains its claim owner", "[storage][drop_ownership]") {
	auto ownership = make_shared_ptr<DropOwnership>(DropActions {29});
	auto owner = make_shared_ptr<DropClaim>();
	duckdb::weak_ptr<DropClaim> owner_reference(owner);

	REQUIRE(ownership->Claim(owner));
	owner.reset();
	REQUIRE(!owner_reference.expired());

	auto retained_owner = owner_reference.lock();
	REQUIRE(retained_owner);
	REQUIRE(ownership->IsClaimedBy(*retained_owner));
	REQUIRE(ownership->Revert(*retained_owner));
	retained_owner.reset();
	REQUIRE(owner_reference.expired());
}
