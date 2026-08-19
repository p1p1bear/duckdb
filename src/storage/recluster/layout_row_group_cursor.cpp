#include "duckdb/storage/recluster/row_group_layout.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

RowGroupCollectionSnapshot::RowGroupCollectionSnapshot(shared_ptr<RowGroupSegmentTree> tree)
    : kind(Kind::BASE_TREE), base_tree(std::move(tree)) {
	if (!base_tree) {
		throw InternalException("A base-tree row group snapshot requires a tree");
	}
}

RowGroupCollectionSnapshot::RowGroupCollectionSnapshot(shared_ptr<const RowGroupLayout> layout_p)
    : kind(Kind::VERSIONED_LAYOUT), layout(std::move(layout_p)) {
	if (!layout) {
		throw InternalException("A versioned row group snapshot requires a layout");
	}
}

const shared_ptr<RowGroupSegmentTree> &RowGroupCollectionSnapshot::GetBaseTree() const {
	switch (kind) {
	case Kind::BASE_TREE:
		return base_tree;
	case Kind::VERSIONED_LAYOUT:
		return layout->base_tree;
	}
	throw InternalException("Unsupported row group snapshot kind");
}

row_t LayoutRowGroupEntry::GetRowEnd() const {
	if (!row_group) {
		throw InternalException("Cannot get the end of an empty row group entry");
	}
	return row_start + NumericCast<row_t>(row_group->count.load());
}

static optional_idx FindContainingPatch(const RowGroupLayout &layout, row_t row_id) {
	idx_t lower = 0;
	idx_t upper = layout.patches.size();
	while (lower < upper) {
		auto index = lower + (upper - lower) / 2;
		if (layout.patches[index]->range.start <= row_id) {
			lower = index + 1;
		} else {
			upper = index;
		}
	}
	if (lower == 0) {
		return optional_idx();
	}
	auto candidate = lower - 1;
	return layout.patches[candidate]->range.Contains(row_id) ? optional_idx(candidate) : optional_idx();
}

static bool LookupBaseTree(RowGroupSegmentTree &tree, row_t row_id, LayoutRowGroupEntry &result) {
	if (row_id < 0) {
		return false;
	}
	idx_t segment_index;
	auto tree_lock = tree.Lock();
	if (!tree.TryGetSegmentIndex(tree_lock, NumericCast<idx_t>(row_id), segment_index)) {
		return false;
	}
	auto node = tree.GetSegmentByIndex(tree_lock, NumericCast<int64_t>(segment_index));
	result.row_group = node->ReferenceNode();
	result.row_start = NumericCast<row_t>(node->GetRowStart());
	result.layout_index = segment_index;
	return true;
}

bool RowGroupCollectionSnapshot::Lookup(row_t row_id, LayoutRowGroupEntry &result) const {
	result = LayoutRowGroupEntry();
	if (kind == Kind::BASE_TREE) {
		return LookupBaseTree(*base_tree, row_id, result);
	}

	auto patch_index = FindContainingPatch(*layout, row_id);
	if (!patch_index.IsValid()) {
		auto found = LookupBaseTree(*layout->base_tree, row_id, result);
		result.layout_index = DConstants::INVALID_INDEX;
		return found;
	}

	auto &patch = *layout->patches[patch_index.GetIndex()];
	auto replacement_start = patch.range.start;
	for (auto &row_group : patch.replacement_groups) {
		auto replacement_end = replacement_start + NumericCast<row_t>(row_group->count.load());
		if (row_id < replacement_end) {
			result.row_group = row_group;
			result.row_start = replacement_start;
			result.layout_index = DConstants::INVALID_INDEX;
			return true;
		}
		replacement_start = replacement_end;
	}
	return false;
}

LayoutRowGroupCursor::LayoutRowGroupCursor(RowGroupCollectionSnapshot snapshot_p, optional<RowGroupRange> scan_range_p)
    : snapshot(std::move(snapshot_p)), scan_range(std::move(scan_range_p)) {
	if (scan_range && (scan_range->start < 0 || scan_range->start > scan_range->end)) {
		throw InternalException("Layout row group cursor has an invalid scan range");
	}
	base_node = snapshot.GetBaseTree()->GetRootSegment();
}

void LayoutRowGroupCursor::AdvanceBase() {
	if (base_node) {
		base_node = snapshot.GetBaseTree()->GetNextSegment(*base_node);
	}
}

void LayoutRowGroupCursor::BeginPatch(const LayoutPatch &patch) {
	if (!base_node || NumericCast<row_t>(base_node->GetRowStart()) != patch.range.start) {
		throw InternalException("Layout patch range does not begin at a base row group boundary");
	}

	idx_t replaced_rows = 0;
	row_t replaced_end = patch.range.start;
	while (base_node && NumericCast<row_t>(base_node->GetRowStart()) < patch.range.end) {
		auto base_start = NumericCast<row_t>(base_node->GetRowStart());
		auto base_end = base_start + NumericCast<row_t>(base_node->GetNode().count.load());
		if (base_start < replaced_end || base_end > patch.range.end) {
			throw InternalException("Layout patch range is not aligned with its base row groups");
		}
		replaced_rows += base_node->GetNode().count.load();
		replaced_end = base_end;
		AdvanceBase();
	}
	if (replaced_end != patch.range.end || replaced_rows != patch.replaced_physical_rows) {
		throw InternalException("Layout patch replaced row count does not match its base row groups");
	}

	replacement_index = 0;
	replacement_row_start = patch.range.start;
	emitting_patch = true;
}

bool LayoutRowGroupCursor::NextUnfiltered(LayoutRowGroupEntry &result) {
	if (snapshot.kind == RowGroupCollectionSnapshot::Kind::BASE_TREE) {
		if (!base_node) {
			return false;
		}
		result.row_group = base_node->ReferenceNode();
		result.row_start = NumericCast<row_t>(base_node->GetRowStart());
		result.layout_index = base_node->GetIndex();
		AdvanceBase();
		return true;
	}

	while (true) {
		if (emitting_patch) {
			auto &patch = *snapshot.layout->patches[patch_index];
			if (replacement_index < patch.replacement_groups.size()) {
				auto &row_group = patch.replacement_groups[replacement_index++];
				result.row_group = row_group;
				result.row_start = replacement_row_start;
				result.layout_index = next_layout_index++;
				replacement_row_start += NumericCast<row_t>(row_group->count.load());
				return true;
			}
			emitting_patch = false;
			patch_index++;
			continue;
		}

		if (patch_index < snapshot.layout->patches.size()) {
			auto &patch = *snapshot.layout->patches[patch_index];
			if (base_node && NumericCast<row_t>(base_node->GetRowStart()) < patch.range.start) {
				auto base_end = NumericCast<row_t>(base_node->GetRowEnd());
				if (base_end > patch.range.start) {
					throw InternalException("Layout patch starts inside a base row group");
				}
				result.row_group = base_node->ReferenceNode();
				result.row_start = NumericCast<row_t>(base_node->GetRowStart());
				result.layout_index = next_layout_index++;
				AdvanceBase();
				return true;
			}
			BeginPatch(patch);
			continue;
		}

		if (!base_node) {
			return false;
		}
		result.row_group = base_node->ReferenceNode();
		result.row_start = NumericCast<row_t>(base_node->GetRowStart());
		result.layout_index = next_layout_index++;
		AdvanceBase();
		return true;
	}
}

bool LayoutRowGroupCursor::Next(LayoutRowGroupEntry &result) {
	if (scan_range && scan_range->start == scan_range->end) {
		return false;
	}
	while (NextUnfiltered(result)) {
		if (!scan_range) {
			return true;
		}
		if (result.GetRowEnd() <= scan_range->start) {
			continue;
		}
		if (result.row_start >= scan_range->end) {
			return false;
		}
		return true;
	}
	return false;
}

} // namespace duckdb
