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

RowGroupLayout::RowGroupLayout(layout_version_t layout_version_p, transaction_t visible_from_p,
                               shared_ptr<RowGroupSegmentTree> base_tree_p,
                               vector<shared_ptr<const LayoutPatch>> patches_p)
    : layout_version(layout_version_p), visible_from(visible_from_p), base_tree(std::move(base_tree_p)),
      patches(std::move(patches_p)) {
	VerifyLayout(*this);
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
	previous.push_back(std::move(current));
	current = std::move(new_layout);
}

void TableLayoutHistory::InstallCheckpointTree(shared_ptr<RowGroupSegmentTree> tree) {
	if (!tree) {
		throw InternalException("Cannot install a null checkpoint row group tree");
	}

	lock_guard<mutex> guard(lock);
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
