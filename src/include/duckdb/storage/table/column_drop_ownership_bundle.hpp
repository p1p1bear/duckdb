//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_drop_ownership_bundle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/table/row_group_column_drop_ownership.hpp"

namespace duckdb {

class ColumnDropOwnershipBundleIdentity;

enum class ColumnDropOwnershipRuntimeKind : uint8_t {
	INVALID = 0,
	STANDARD,
	VALIDITY,
	LIST,
	STRUCT,
	ARRAY,
	VARIANT,
	GEOMETRY
};

enum class ColumnDropOwnershipChildRole : uint8_t {
	INVALID = 0,
	ROOT,
	VALIDITY,
	ELEMENT,
	STRUCT_FIELD,
	VARIANT_CHILD,
	GEOMETRY_STORAGE
};

struct ColumnDropOwnershipLayoutTag {
	ColumnDropOwnershipLayoutTag(ColumnDropOwnershipRuntimeKind runtime_kind, LogicalType logical_type,
	                             uint64_t layout_value);

	ColumnDropOwnershipRuntimeKind runtime_kind;
	LogicalType logical_type;
	//! Runtime layout discriminator. Only VARIANT and GEOMETRY interpret this value.
	uint64_t layout_value;
};

struct ColumnDropOwnershipChildKey {
	ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole role, idx_t ordinal);

	bool operator==(const ColumnDropOwnershipChildKey &other) const noexcept {
		return role == other.role && ordinal == other.ordinal;
	}
	bool operator!=(const ColumnDropOwnershipChildKey &other) const noexcept {
		return !(*this == other);
	}

	ColumnDropOwnershipChildRole role;
	idx_t ordinal;
};

struct ColumnDropOwnershipNodeDescriptor {
	ColumnDropOwnershipNodeDescriptor(ColumnDropOwnershipLayoutTag layout_tag, ColumnDropOwnershipChildKey child_key,
	                                  idx_t parent_index,
	                                  shared_ptr<RowGroupColumnDropOwnership> direct_token = nullptr);

	ColumnDropOwnershipLayoutTag layout_tag;
	ColumnDropOwnershipChildKey child_key;
	idx_t parent_index;
	//! An observed existing token. Capture creates a fresh token only when this is null.
	shared_ptr<RowGroupColumnDropOwnership> direct_token;
};

class ColumnDropOwnershipShape {
public:
	static constexpr idx_t ROOT_PARENT_INDEX = DConstants::INVALID_INDEX;

	static shared_ptr<const ColumnDropOwnershipShape> Capture(vector<ColumnDropOwnershipNodeDescriptor> observed_tree);

	ColumnDropOwnershipShape(const ColumnDropOwnershipShape &) = delete;
	ColumnDropOwnershipShape &operator=(const ColumnDropOwnershipShape &) = delete;
	ColumnDropOwnershipShape(ColumnDropOwnershipShape &&) noexcept;
	ColumnDropOwnershipShape &operator=(ColumnDropOwnershipShape &&) = delete;

	idx_t NodeCount() const noexcept {
		return ordered_nodes.size();
	}
	const vector<ColumnDropOwnershipNodeDescriptor> &GetNodes() const noexcept {
		return ordered_nodes;
	}

private:
	ColumnDropOwnershipShape(vector<ColumnDropOwnershipNodeDescriptor> ordered_nodes,
	                         vector<uint8_t> had_existing_token);
	bool TryAffiliate(const shared_ptr<ColumnDropOwnershipBundleIdentity> &identity) const;
	bool HasExistingTokens() const noexcept;

private:
	vector<ColumnDropOwnershipNodeDescriptor> ordered_nodes;
	vector<uint8_t> had_existing_token;
	mutable mutex affiliation_lock;
	mutable shared_ptr<ColumnDropOwnershipBundleIdentity> affiliation;

	friend class ColumnDropOwnershipBundle;
};

enum class ColumnDropOwnershipBindResult : uint8_t { ADOPTED, VERIFIED, MISMATCH };

class ColumnDropOwnershipBundle {
public:
	ColumnDropOwnershipBundle();
	ColumnDropOwnershipBundle(const ColumnDropOwnershipBundle &) = delete;
	ColumnDropOwnershipBundle &operator=(const ColumnDropOwnershipBundle &) = delete;
	ColumnDropOwnershipBundle(ColumnDropOwnershipBundle &&) = delete;
	ColumnDropOwnershipBundle &operator=(ColumnDropOwnershipBundle &&) = delete;

public:
	//! Initializes a fresh bundle before publication, including candidates with observed existing tokens.
	//! Throws without changing canonical_tokens if this bundle or candidate already belongs elsewhere.
	void Initialize(const shared_ptr<const ColumnDropOwnershipShape> &candidate,
	                vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens);
	//! canonical_tokens must be pre-sized to the candidate node count. Mismatch leaves it unchanged.
	ColumnDropOwnershipBindResult Bind(const shared_ptr<const ColumnDropOwnershipShape> &candidate,
	                                   vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) noexcept;

private:
	shared_ptr<ColumnDropOwnershipBundleIdentity> identity;
	mutex bind_lock;
	shared_ptr<const ColumnDropOwnershipShape> bound_shape;
};

} // namespace duckdb
