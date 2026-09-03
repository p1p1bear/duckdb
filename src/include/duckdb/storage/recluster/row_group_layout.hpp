//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/row_group_layout.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

class RowGroup;
class RowGroupSegmentTree;
struct LayoutRowGroupEntry;
template <class T>
struct SegmentNode;

struct RowGroupRange {
	row_t start;
	row_t end;

	bool Contains(row_t row_id) const {
		return row_id >= start && row_id < end;
	}

	bool Overlaps(const RowGroupRange &other) const {
		return start < other.end && other.start < end;
	}
};

struct LayoutPatch {
	recluster_task_id_t task_id = hugeint_t(0, 0);
	RowGroupRange range {0, 0};
	sort_order_id_t sort_order_id = INVALID_SORT_ORDER_ID;
	sort_run_id_t run_id = INVALID_SORT_RUN_ID;
	idx_t replaced_physical_rows = 0;
	idx_t replacement_physical_rows = 0;
	vector<shared_ptr<RowGroup>> replacement_groups;
};

struct LayoutPatchIndex {
	idx_t base_start_index;
	idx_t base_end_index;
	idx_t layout_start_index;
	vector<row_t> replacement_starts;
};

struct RowGroupLayout {
	RowGroupLayout(layout_version_t layout_version, transaction_t visible_from,
	               shared_ptr<RowGroupSegmentTree> base_tree, vector<shared_ptr<const LayoutPatch>> patches = {});
	optional_idx FindPatch(row_t row_id) const;
	idx_t FindNextPatch(row_t row_id) const;
	optional_idx FindReplacementGroup(idx_t patch_index, row_t row_id) const;
	idx_t GetBaseLayoutIndex(idx_t base_index, idx_t next_patch_index) const;

	layout_version_t layout_version;
	transaction_t visible_from;
	shared_ptr<RowGroupSegmentTree> base_tree;
	vector<shared_ptr<const LayoutPatch>> patches;
	vector<LayoutPatchIndex> patch_indexes;
};

static constexpr idx_t MAX_LAYOUT_PATCHES_PER_CHECKPOINT = 64;

class TableLayoutHistory {
public:
	explicit TableLayoutHistory(shared_ptr<const RowGroupLayout> initial_layout);

	shared_ptr<const RowGroupLayout> GetCurrent() const;
	shared_ptr<const RowGroupLayout> GetForTransaction(transaction_t start_time) const;

	void Publish(shared_ptr<const RowGroupLayout> new_layout);
	void RevertPublished(const shared_ptr<const RowGroupLayout> &published_layout,
	                     const shared_ptr<const RowGroupLayout> &previous_layout);
	void InstallCheckpointTree(shared_ptr<RowGroupSegmentTree> tree,
	                           shared_ptr<const RowGroupLayout> expected_layout = nullptr);
	void Cleanup(transaction_t oldest_active_start);

private:
	mutable mutex lock;
	shared_ptr<const RowGroupLayout> current;
	deque<shared_ptr<const RowGroupLayout>> previous;
};

struct RowGroupCollectionSnapshot {
	enum class Kind : uint8_t { BASE_TREE, VERSIONED_LAYOUT };

	explicit RowGroupCollectionSnapshot(shared_ptr<RowGroupSegmentTree> tree);
	explicit RowGroupCollectionSnapshot(shared_ptr<const RowGroupLayout> layout);

	const shared_ptr<RowGroupSegmentTree> &GetBaseTree() const;
	bool HasPatch(const RowGroupRange &range) const;
	bool Lookup(row_t row_id, LayoutRowGroupEntry &result) const;

	Kind kind;
	shared_ptr<RowGroupSegmentTree> base_tree;
	shared_ptr<const RowGroupLayout> layout;
};

struct LayoutRowGroupEntry {
	shared_ptr<RowGroup> row_group;
	row_t row_start = 0;
	//! VERSIONED_LAYOUT point lookups leave this invalid because preceding patches can shift the merged index.
	idx_t layout_index = DConstants::INVALID_INDEX;

	row_t GetRowEnd() const;
};

class LayoutRowGroupCursor {
public:
	LayoutRowGroupCursor(RowGroupCollectionSnapshot snapshot, optional<RowGroupRange> scan_range = nullopt);

	bool Next(LayoutRowGroupEntry &result);

private:
	bool NextUnfiltered(LayoutRowGroupEntry &result);
	void AdvanceBase();
	void BeginPatch(idx_t index);
	void Seek(row_t row_id);

private:
	RowGroupCollectionSnapshot snapshot;
	optional<RowGroupRange> scan_range;
	optional_ptr<SegmentNode<RowGroup>> base_node;
	idx_t patch_index = 0;
	idx_t replacement_index = 0;
	idx_t next_layout_index = 0;
	row_t replacement_row_start = 0;
	bool emitting_patch = false;
};

} // namespace duckdb
