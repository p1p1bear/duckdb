//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_drop_ownership_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_drop_ownership_bundle.hpp"

namespace duckdb {

class ColumnData;

class ColumnDropOwnershipChildVisitor {
public:
	virtual ~ColumnDropOwnershipChildVisitor() = default;

	virtual void Visit(const ColumnDropOwnershipChildKey &key, ColumnData &column) = 0;
};

struct ColumnDropOwnershipRuntimeTree {
	shared_ptr<const ColumnDropOwnershipShape> shape;
	vector<reference<ColumnData>> nodes;

	//! Applies a pre-sized canonical token plan without partially modifying the tree.
	bool ApplyTokenPlan(const vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) noexcept;
};

ColumnDropOwnershipRuntimeTree CaptureColumnDropOwnershipRuntimeTree(ColumnData &root);

} // namespace duckdb
