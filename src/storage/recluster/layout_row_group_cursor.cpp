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

bool RowGroupCollectionSnapshot::HasPatch(const RowGroupRange &range) const {
	D_ASSERT(range.start <= range.end);
	if (kind != Kind::VERSIONED_LAYOUT || range.start == range.end) {
		return false;
	}
	if (layout->FindPatch(range.start).IsValid()) {
		return true;
	}
	auto next_patch = layout->FindNextPatch(range.start);
	return next_patch < layout->patches.size() && layout->patches[next_patch]->range.start < range.end;
}

row_t LayoutRowGroupEntry::GetRowEnd() const {
	if (!row_group) {
		throw InternalException("Cannot get the end of an empty row group entry");
	}
	return row_start + NumericCast<row_t>(row_group->count.load());
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

	auto patch_index = layout->FindPatch(row_id);
	if (!patch_index.IsValid()) {
		auto found = LookupBaseTree(*layout->base_tree, row_id, result);
		result.layout_index = DConstants::INVALID_INDEX;
		return found;
	}

	auto replacement_index = layout->FindReplacementGroup(patch_index.GetIndex(), row_id);
	if (replacement_index.IsValid()) {
		auto &index = layout->patch_indexes[patch_index.GetIndex()];
		result.row_group = layout->patches[patch_index.GetIndex()]->replacement_groups[replacement_index.GetIndex()];
		result.row_start = index.replacement_starts[replacement_index.GetIndex()];
		result.layout_index = DConstants::INVALID_INDEX;
		return true;
	}
	return false;
}

LayoutRowGroupCursor::LayoutRowGroupCursor(RowGroupCollectionSnapshot snapshot_p, optional<RowGroupRange> scan_range_p)
    : snapshot(std::move(snapshot_p)), scan_range(std::move(scan_range_p)) {
	if (scan_range && (scan_range->start < 0 || scan_range->start > scan_range->end)) {
		throw InternalException("Layout row group cursor has an invalid scan range");
	}
	if (!scan_range || scan_range->start != scan_range->end) {
		Seek(scan_range ? scan_range->start : 0);
	}
}

void LayoutRowGroupCursor::AdvanceBase() {
	if (base_node) {
		base_node = snapshot.GetBaseTree()->GetNextSegment(*base_node);
	}
}

void LayoutRowGroupCursor::BeginPatch(idx_t index) {
	auto &patch = *snapshot.layout->patches[index];
	auto &navigation = snapshot.layout->patch_indexes[index];
	if (!base_node || base_node->GetIndex() != navigation.base_start_index) {
		throw InternalException("Layout patch range does not begin at a base row group boundary");
	}
	base_node = snapshot.GetBaseTree()->GetSegmentByIndex(NumericCast<int64_t>(navigation.base_end_index));
	replacement_index = 0;
	replacement_row_start = patch.range.start;
	emitting_patch = true;
}

void LayoutRowGroupCursor::Seek(row_t row_id) {
	auto &tree = *snapshot.GetBaseTree();
	if (snapshot.kind == RowGroupCollectionSnapshot::Kind::BASE_TREE) {
		base_node = tree.GetSegmentAtOrAfter(NumericCast<idx_t>(row_id));
		return;
	}

	auto containing_patch = snapshot.layout->FindPatch(row_id);
	if (containing_patch.IsValid()) {
		patch_index = containing_patch.GetIndex();
		auto &navigation = snapshot.layout->patch_indexes[patch_index];
		base_node = tree.GetSegmentByIndex(NumericCast<int64_t>(navigation.base_end_index));
		auto replacement = snapshot.layout->FindReplacementGroup(patch_index, row_id);
		if (replacement.IsValid()) {
			replacement_index = replacement.GetIndex();
			replacement_row_start = navigation.replacement_starts[replacement_index];
			next_layout_index = navigation.layout_start_index + replacement_index;
			emitting_patch = true;
			return;
		}
		patch_index++;
		next_layout_index = navigation.layout_start_index + navigation.replacement_starts.size();
		return;
	}

	base_node = tree.GetSegmentAtOrAfter(NumericCast<idx_t>(row_id));
	patch_index = snapshot.layout->FindNextPatch(row_id);
	if (!base_node) {
		return;
	}
	while (patch_index < snapshot.layout->patches.size() &&
	       snapshot.layout->patches[patch_index]->range.end <= NumericCast<row_t>(base_node->GetRowStart())) {
		patch_index++;
	}
	next_layout_index = snapshot.layout->GetBaseLayoutIndex(base_node->GetIndex(), patch_index);
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
			BeginPatch(patch_index);
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
