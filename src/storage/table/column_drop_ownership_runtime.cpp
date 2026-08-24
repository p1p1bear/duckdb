#include "duckdb/storage/table/column_drop_ownership_runtime.hpp"

#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

bool ColumnDropOwnershipRuntimeTree::ApplyTokenPlan(
    const vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) noexcept {
	if (!shape || nodes.size() != shape->NodeCount() || canonical_tokens.size() != nodes.size()) {
		return false;
	}
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		auto &node = nodes[node_index].get();
		auto &token = canonical_tokens[node_index];
		if (!token || (node.GetDropOwnershipToken() && node.GetDropOwnershipToken() != token)) {
			return false;
		}
	}
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		nodes[node_index].get().SetDropOwnershipToken(canonical_tokens[node_index]);
	}
	return true;
}

class ColumnDropOwnershipRuntimeCapture : public ColumnDropOwnershipChildVisitor {
public:
	ColumnDropOwnershipRuntimeTree Capture(ColumnData &root) {
		CaptureNode(root, ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::ROOT, 0),
		            ColumnDropOwnershipShape::ROOT_PARENT_INDEX);
		ColumnDropOwnershipRuntimeTree result;
		result.shape = ColumnDropOwnershipShape::Capture(std::move(descriptors));
		result.nodes = std::move(nodes);
		return result;
	}

	void Visit(const ColumnDropOwnershipChildKey &key, ColumnData &column) override {
		CaptureNode(column, key, current_parent);
	}

private:
	void CaptureNode(ColumnData &column, const ColumnDropOwnershipChildKey &key, idx_t parent_index) {
		for (auto &observed_node : nodes) {
			if (&observed_node.get() == &column) {
				throw InternalException("Column drop ownership runtime tree contains a repeated node");
			}
		}
		auto node_index = descriptors.size();
		descriptors.emplace_back(ColumnDropOwnershipLayoutTag(column.GetDropOwnershipRuntimeKind(), column.GetType(),
		                                                      column.GetDropOwnershipLayoutValue()),
		                         key, parent_index, column.GetDropOwnershipToken());
		nodes.emplace_back(column);
		auto previous_parent = current_parent;
		current_parent = node_index;
		column.VisitDropOwnershipChildren(*this);
		current_parent = previous_parent;
	}

private:
	vector<ColumnDropOwnershipNodeDescriptor> descriptors;
	vector<reference<ColumnData>> nodes;
	idx_t current_parent = ColumnDropOwnershipShape::ROOT_PARENT_INDEX;
};

ColumnDropOwnershipRuntimeTree CaptureColumnDropOwnershipRuntimeTree(ColumnData &root) {
	ColumnDropOwnershipRuntimeCapture capture;
	return capture.Capture(root);
}

} // namespace duckdb
