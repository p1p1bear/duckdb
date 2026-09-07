#include "duckdb/storage/recluster/row_group_layout.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

static void VerifyPatch(const LayoutPatch &patch) {
	if (patch.range.start < 0 || patch.range.start >= patch.range.end) {
		throw InternalException("Layout patch has an invalid row group range");
	}
	if (patch.task_id == hugeint_t(0, 0)) {
		throw InternalException("Layout patch has an invalid task ID");
	}
	if (patch.sort_order_id == INVALID_SORT_ORDER_ID || patch.run_id == INVALID_SORT_RUN_ID) {
		throw InternalException("Layout patch has invalid sort metadata");
	}
	auto range_size = NumericCast<idx_t>(patch.range.end - patch.range.start);
	if (patch.replaced_physical_rows == 0 || patch.replaced_physical_rows > range_size) {
		throw InternalException("Layout patch has an invalid replaced row count");
	}
	if (patch.replacement_physical_rows > range_size) {
		throw InternalException("Layout patch has an invalid replacement row count");
	}

	idx_t replacement_count = 0;
	for (auto &row_group : patch.replacement_groups) {
		if (!row_group) {
			throw InternalException("Layout patch contains a null replacement row group");
		}
		auto row_group_count = row_group->count.load();
		if (row_group_count > range_size - replacement_count) {
			throw InternalException("Layout patch replacement row groups exceed its range");
		}
		replacement_count += row_group_count;
	}
	if (replacement_count != patch.replacement_physical_rows) {
		throw InternalException("Layout patch replacement row count does not match its row groups");
	}
}

static void VerifyLayout(const RowGroupLayout &layout) {
	if (!layout.base_tree) {
		throw InternalException("Row group layout requires a base tree");
	}
	if (layout.patches.size() > MAX_LAYOUT_PATCHES_PER_CHECKPOINT) {
		throw InternalException("Row group layout exceeds the checkpoint patch limit");
	}

	optional<RowGroupRange> previous_range;
	for (auto &patch : layout.patches) {
		if (!patch) {
			throw InternalException("Row group layout contains a null patch");
		}
		VerifyPatch(*patch);
		if (previous_range && (patch->range.start < previous_range->end)) {
			throw InternalException("Row group layout patches are not sorted or overlap");
		}
		previous_range = patch->range;
	}
}

static vector<LayoutPatchIndex> BuildPatchIndexes(RowGroupSegmentTree &base_tree,
                                                  const vector<shared_ptr<const LayoutPatch>> &patches) {
	vector<LayoutPatchIndex> result;
	result.reserve(patches.size());
	idx_t replaced_group_count = 0;
	idx_t replacement_group_count = 0;
	auto tree_lock = base_tree.Lock();
	for (auto &patch : patches) {
		auto base_node = base_tree.GetSegmentAtOrAfter(tree_lock, NumericCast<idx_t>(patch->range.start));
		if (!base_node || NumericCast<row_t>(base_node->GetRowStart()) != patch->range.start) {
			throw InternalException("Layout patch range does not begin at a base row group boundary");
		}

		LayoutPatchIndex index;
		index.base_start_index = base_node->GetIndex();
		index.layout_start_index = index.base_start_index - replaced_group_count + replacement_group_count;
		idx_t replaced_rows = 0;
		row_t replaced_end = patch->range.start;
		while (base_node && NumericCast<row_t>(base_node->GetRowStart()) < patch->range.end) {
			auto base_start = NumericCast<row_t>(base_node->GetRowStart());
			auto base_end = NumericCast<row_t>(base_node->GetRowEnd());
			if (base_start < replaced_end || base_end > patch->range.end) {
				throw InternalException("Layout patch range is not aligned with its base row groups");
			}
			replaced_rows += base_node->GetNode().count.load();
			replaced_end = base_end;
			base_node = base_tree.GetNextSegment(tree_lock, *base_node);
		}
		if (replaced_end != patch->range.end || replaced_rows != patch->replaced_physical_rows) {
			throw InternalException("Layout patch replaced row count does not match its base row groups");
		}
		index.base_end_index = base_node ? base_node->GetIndex() : base_tree.GetSegmentCount(tree_lock);

		row_t replacement_start = patch->range.start;
		index.replacement_starts.reserve(patch->replacement_groups.size());
		for (auto &row_group : patch->replacement_groups) {
			index.replacement_starts.push_back(replacement_start);
			replacement_start += NumericCast<row_t>(row_group->count.load());
		}
		replaced_group_count += index.base_end_index - index.base_start_index;
		replacement_group_count += patch->replacement_groups.size();
		result.push_back(std::move(index));
	}
	return result;
}

RowGroupLayout::RowGroupLayout(layout_version_t layout_version_p, transaction_t visible_from_p,
                               shared_ptr<RowGroupSegmentTree> base_tree_p,
                               vector<shared_ptr<const LayoutPatch>> patches_p)
    : layout_version(layout_version_p), visible_from(visible_from_p), base_tree(std::move(base_tree_p)),
      patches(std::move(patches_p)) {
	VerifyLayout(*this);
	patch_indexes = BuildPatchIndexes(*base_tree, patches);
}

optional_idx RowGroupLayout::FindPatch(row_t row_id) const {
	idx_t lower = 0;
	idx_t upper = patches.size();
	while (lower < upper) {
		auto index = lower + (upper - lower) / 2;
		if (patches[index]->range.start <= row_id) {
			lower = index + 1;
		} else {
			upper = index;
		}
	}
	if (lower == 0) {
		return optional_idx();
	}
	auto candidate = lower - 1;
	return patches[candidate]->range.Contains(row_id) ? optional_idx(candidate) : optional_idx();
}

idx_t RowGroupLayout::FindNextPatch(row_t row_id) const {
	idx_t lower = 0;
	idx_t upper = patches.size();
	while (lower < upper) {
		auto index = lower + (upper - lower) / 2;
		if (patches[index]->range.start < row_id) {
			lower = index + 1;
		} else {
			upper = index;
		}
	}
	return lower;
}

optional_idx RowGroupLayout::FindReplacementGroup(idx_t patch_index, row_t row_id) const {
	D_ASSERT(patch_index < patches.size());
	auto &starts = patch_indexes[patch_index].replacement_starts;
	idx_t lower = 0;
	idx_t upper = starts.size();
	while (lower < upper) {
		auto index = lower + (upper - lower) / 2;
		if (starts[index] <= row_id) {
			lower = index + 1;
		} else {
			upper = index;
		}
	}
	if (lower == 0) {
		return optional_idx();
	}
	auto candidate = lower - 1;
	auto row_end =
	    starts[candidate] + NumericCast<row_t>(patches[patch_index]->replacement_groups[candidate]->count.load());
	return row_id < row_end ? optional_idx(candidate) : optional_idx();
}

idx_t RowGroupLayout::GetBaseLayoutIndex(idx_t base_index, idx_t next_patch_index) const {
	D_ASSERT(next_patch_index <= patch_indexes.size());
	if (next_patch_index == 0) {
		return base_index;
	}
	auto &previous = patch_indexes[next_patch_index - 1];
	D_ASSERT(base_index >= previous.base_end_index);
	return previous.layout_start_index + previous.replacement_starts.size() + base_index - previous.base_end_index;
}

TableLayoutHistory::TableLayoutHistory(shared_ptr<const RowGroupLayout> initial_layout)
    : current(std::move(initial_layout)) {
	if (!current) {
		throw InternalException("Table layout history requires an initial layout");
	}
	VerifyLayout(*current);
}

shared_ptr<const RowGroupLayout> TableLayoutHistory::GetCurrent() const {
	lock_guard<mutex> guard(lock);
	return current;
}

shared_ptr<const RowGroupLayout> TableLayoutHistory::GetForTransaction(transaction_t start_time) const {
	lock_guard<mutex> guard(lock);
	if (start_time >= current->visible_from) {
		return current;
	}
	for (auto entry = previous.rbegin(); entry != previous.rend(); entry++) {
		if (start_time >= (*entry)->visible_from) {
			return *entry;
		}
	}
	throw InternalException("No row group layout exists for transaction start time %llu", start_time);
}

void TableLayoutHistory::Publish(shared_ptr<const RowGroupLayout> new_layout) {
	if (!new_layout) {
		throw InternalException("Cannot publish a null row group layout");
	}
	VerifyLayout(*new_layout);

	lock_guard<mutex> guard(lock);
	if (new_layout->visible_from <= current->visible_from) {
		throw InternalException("A row group layout must be published after the current layout");
	}
	if (new_layout->layout_version <= current->layout_version ||
	    new_layout->layout_version - current->layout_version != 1) {
		throw InternalException("A row group layout must advance the layout version by one");
	}
	if (new_layout->base_tree.get() != current->base_tree.get()) {
		throw InternalException("A row group layout cannot publish against a stale checkpoint tree");
	}
	previous.push_back(std::move(current));
	current = std::move(new_layout);
}

void TableLayoutHistory::RevertPublished(const shared_ptr<const RowGroupLayout> &published_layout,
                                         const shared_ptr<const RowGroupLayout> &previous_layout) {
	if (!published_layout || !previous_layout) {
		throw InternalException("Cannot revert a row group layout with a null identity");
	}

	lock_guard<mutex> guard(lock);
	if (current.get() != published_layout.get() || previous.empty() || previous.back().get() != previous_layout.get()) {
		throw InternalException("Cannot revert a row group layout that is no longer current");
	}
	current = std::move(previous.back());
	previous.pop_back();
}

void TableLayoutHistory::InstallCheckpointTree(shared_ptr<RowGroupSegmentTree> tree,
                                               shared_ptr<const RowGroupLayout> expected_layout) {
	if (!tree) {
		throw InternalException("Cannot install a null checkpoint row group tree");
	}

	lock_guard<mutex> guard(lock);
	if (expected_layout && current.get() != expected_layout.get()) {
		throw InternalException("Cannot install a checkpoint tree for a stale row group layout");
	}
	current = make_shared_ptr<RowGroupLayout>(current->layout_version, current->visible_from, std::move(tree));
}

void TableLayoutHistory::Cleanup(transaction_t oldest_active_start) {
	lock_guard<mutex> guard(lock);
	while (!previous.empty()) {
		auto retired_at = previous.size() == 1 ? current->visible_from : previous[1]->visible_from;
		if (oldest_active_start < retired_at) {
			break;
		}
		previous.pop_front();
	}
}

} // namespace duckdb
