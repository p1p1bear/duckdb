#include "duckdb/storage/table/column_drop_ownership_bundle.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"

namespace duckdb {

ColumnDropOwnershipLayoutTag::ColumnDropOwnershipLayoutTag(ColumnDropOwnershipRuntimeKind runtime_kind_p,
                                                           LogicalType logical_type_p, uint64_t layout_value_p)
    : runtime_kind(runtime_kind_p), logical_type(std::move(logical_type_p)), layout_value(layout_value_p) {
}

ColumnDropOwnershipChildKey::ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole role_p, idx_t ordinal_p)
    : role(role_p), ordinal(ordinal_p) {
}

ColumnDropOwnershipNodeDescriptor::ColumnDropOwnershipNodeDescriptor(
    ColumnDropOwnershipLayoutTag layout_tag_p, ColumnDropOwnershipChildKey child_key_p, idx_t parent_index_p,
    shared_ptr<RowGroupColumnDropOwnership> direct_token_p)
    : layout_tag(std::move(layout_tag_p)), child_key(child_key_p), parent_index(parent_index_p),
      direct_token(std::move(direct_token_p)) {
}

static bool IsValidGeometryStorageValue(uint64_t layout_value) {
	switch (layout_value) {
	case static_cast<uint64_t>(GeometryStorageType::SPATIAL):
	case static_cast<uint64_t>(GeometryStorageType::WKB):
	case static_cast<uint64_t>(GeometryStorageType::POINT_XY):
	case static_cast<uint64_t>(GeometryStorageType::LINESTRING_XY):
	case static_cast<uint64_t>(GeometryStorageType::POLYGON_XY):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOINT_XY):
	case static_cast<uint64_t>(GeometryStorageType::MULTILINESTRING_XY):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOLYGON_XY):
	case static_cast<uint64_t>(GeometryStorageType::POINT_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::LINESTRING_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::POLYGON_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOINT_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::MULTILINESTRING_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOLYGON_XYZ):
	case static_cast<uint64_t>(GeometryStorageType::POINT_XYM):
	case static_cast<uint64_t>(GeometryStorageType::LINESTRING_XYM):
	case static_cast<uint64_t>(GeometryStorageType::POLYGON_XYM):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOINT_XYM):
	case static_cast<uint64_t>(GeometryStorageType::MULTILINESTRING_XYM):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOLYGON_XYM):
	case static_cast<uint64_t>(GeometryStorageType::POINT_XYZM):
	case static_cast<uint64_t>(GeometryStorageType::LINESTRING_XYZM):
	case static_cast<uint64_t>(GeometryStorageType::POLYGON_XYZM):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOINT_XYZM):
	case static_cast<uint64_t>(GeometryStorageType::MULTILINESTRING_XYZM):
	case static_cast<uint64_t>(GeometryStorageType::MULTIPOLYGON_XYZM):
		return true;
	default:
		return false;
	}
}

static bool IsValidLayoutTag(const ColumnDropOwnershipLayoutTag &tag) {
	if (tag.logical_type.id() == LogicalTypeId::INVALID) {
		return false;
	}
	switch (tag.runtime_kind) {
	case ColumnDropOwnershipRuntimeKind::STANDARD:
		return tag.layout_value == 0 && tag.logical_type.id() != LogicalTypeId::VALIDITY &&
		       tag.logical_type.id() != LogicalTypeId::VARIANT &&
		       tag.logical_type.InternalType() != PhysicalType::STRUCT &&
		       tag.logical_type.InternalType() != PhysicalType::LIST &&
		       tag.logical_type.InternalType() != PhysicalType::ARRAY;
	case ColumnDropOwnershipRuntimeKind::VALIDITY:
		return tag.layout_value == 0 && tag.logical_type.id() == LogicalTypeId::VALIDITY;
	case ColumnDropOwnershipRuntimeKind::LIST:
		return tag.layout_value == 0 && tag.logical_type.InternalType() == PhysicalType::LIST;
	case ColumnDropOwnershipRuntimeKind::STRUCT:
		return tag.layout_value == 0 && tag.logical_type.InternalType() == PhysicalType::STRUCT &&
		       tag.logical_type.id() != LogicalTypeId::VARIANT;
	case ColumnDropOwnershipRuntimeKind::ARRAY:
		return tag.layout_value == 0 && tag.logical_type.InternalType() == PhysicalType::ARRAY;
	case ColumnDropOwnershipRuntimeKind::VARIANT:
		return tag.logical_type.id() == LogicalTypeId::VARIANT && tag.layout_value <= 1;
	case ColumnDropOwnershipRuntimeKind::GEOMETRY:
		return tag.logical_type.id() == LogicalTypeId::GEOMETRY && IsValidGeometryStorageValue(tag.layout_value);
	case ColumnDropOwnershipRuntimeKind::INVALID:
		return false;
	}
	return false;
}

static bool IsValidChildKey(const ColumnDropOwnershipChildKey &key) {
	if (key.ordinal == DConstants::INVALID_INDEX) {
		return false;
	}
	switch (key.role) {
	case ColumnDropOwnershipChildRole::ROOT:
	case ColumnDropOwnershipChildRole::VALIDITY:
	case ColumnDropOwnershipChildRole::ELEMENT:
	case ColumnDropOwnershipChildRole::GEOMETRY_STORAGE:
		return key.ordinal == 0;
	case ColumnDropOwnershipChildRole::STRUCT_FIELD:
		return true;
	case ColumnDropOwnershipChildRole::VARIANT_CHILD:
		return key.ordinal <= 1;
	case ColumnDropOwnershipChildRole::INVALID:
		return false;
	}
	return false;
}

static idx_t ExpectedDirectChildCount(const ColumnDropOwnershipNodeDescriptor &parent) {
	switch (parent.layout_tag.runtime_kind) {
	case ColumnDropOwnershipRuntimeKind::STANDARD:
		return 1;
	case ColumnDropOwnershipRuntimeKind::VALIDITY:
		return 0;
	case ColumnDropOwnershipRuntimeKind::LIST:
	case ColumnDropOwnershipRuntimeKind::ARRAY:
		return 2;
	case ColumnDropOwnershipRuntimeKind::STRUCT:
		return 1 + StructType::GetChildTypes(parent.layout_tag.logical_type).size();
	case ColumnDropOwnershipRuntimeKind::VARIANT:
		return 2 + parent.layout_tag.layout_value;
	case ColumnDropOwnershipRuntimeKind::GEOMETRY:
		return 1;
	case ColumnDropOwnershipRuntimeKind::INVALID:
		return 0;
	}
	return 0;
}

static bool IsExpectedDirectChild(const ColumnDropOwnershipNodeDescriptor &parent,
                                  const ColumnDropOwnershipNodeDescriptor &child, idx_t child_position) {
	ColumnDropOwnershipChildKey expected(ColumnDropOwnershipChildRole::INVALID, 0);
	switch (parent.layout_tag.runtime_kind) {
	case ColumnDropOwnershipRuntimeKind::STANDARD:
		expected = ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VALIDITY, 0);
		break;
	case ColumnDropOwnershipRuntimeKind::LIST:
	case ColumnDropOwnershipRuntimeKind::ARRAY:
		expected = child_position == 0 ? ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VALIDITY, 0)
		                               : ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::ELEMENT, 0);
		break;
	case ColumnDropOwnershipRuntimeKind::STRUCT:
		expected = child_position == 0
		               ? ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VALIDITY, 0)
		               : ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::STRUCT_FIELD, child_position - 1);
		break;
	case ColumnDropOwnershipRuntimeKind::VARIANT:
		expected = child_position == 0
		               ? ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VALIDITY, 0)
		               : ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::VARIANT_CHILD, child_position - 1);
		break;
	case ColumnDropOwnershipRuntimeKind::GEOMETRY:
		expected = ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::GEOMETRY_STORAGE, 0);
		break;
	case ColumnDropOwnershipRuntimeKind::VALIDITY:
	case ColumnDropOwnershipRuntimeKind::INVALID:
		return false;
	}
	return child.child_key == expected;
}

static bool ChildTypeMatchesParent(const ColumnDropOwnershipNodeDescriptor &parent,
                                   const ColumnDropOwnershipNodeDescriptor &child) {
	switch (child.child_key.role) {
	case ColumnDropOwnershipChildRole::VALIDITY:
		return child.layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::VALIDITY;
	case ColumnDropOwnershipChildRole::ELEMENT:
		if (parent.layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::LIST) {
			return child.layout_tag.logical_type == ListType::GetChildType(parent.layout_tag.logical_type);
		}
		return child.layout_tag.logical_type == ArrayType::GetChildType(parent.layout_tag.logical_type);
	case ColumnDropOwnershipChildRole::STRUCT_FIELD: {
		auto &children = StructType::GetChildTypes(parent.layout_tag.logical_type);
		return child.child_key.ordinal < children.size() &&
		       child.layout_tag.logical_type == children[child.child_key.ordinal].second;
	}
	case ColumnDropOwnershipChildRole::VARIANT_CHILD:
		return child.child_key.ordinal != 0 || child.layout_tag.logical_type == VariantShredding::GetUnshreddedType();
	case ColumnDropOwnershipChildRole::GEOMETRY_STORAGE: {
		auto storage_type = static_cast<GeometryStorageType>(parent.layout_tag.layout_value);
		if (storage_type == GeometryStorageType::WKB) {
			return child.layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::STANDARD &&
			       child.layout_tag.logical_type == parent.layout_tag.logical_type;
		}
		if (storage_type == GeometryStorageType::SPATIAL) {
			return child.layout_tag.runtime_kind == ColumnDropOwnershipRuntimeKind::STANDARD &&
			       (child.layout_tag.logical_type == parent.layout_tag.logical_type ||
			        child.layout_tag.logical_type == Geometry::GetSpatialGeometryType());
		}
		return child.layout_tag.logical_type == Geometry::GetVectorizedType(storage_type);
	}
	case ColumnDropOwnershipChildRole::ROOT:
	case ColumnDropOwnershipChildRole::INVALID:
		return false;
	}
	return false;
}

static bool IsValidStandardGeometryContext(const vector<ColumnDropOwnershipNodeDescriptor> &nodes, idx_t node_index) {
	auto &node = nodes[node_index];
	if (node.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::STANDARD ||
	    node.layout_tag.logical_type.id() != LogicalTypeId::GEOMETRY) {
		return true;
	}
	if (node_index == 0 || node.child_key.role != ColumnDropOwnershipChildRole::GEOMETRY_STORAGE ||
	    node.parent_index >= node_index) {
		return false;
	}
	auto &parent = nodes[node.parent_index];
	if (parent.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::GEOMETRY) {
		return false;
	}
	auto storage_type = static_cast<GeometryStorageType>(parent.layout_tag.layout_value);
	return storage_type == GeometryStorageType::WKB || storage_type == GeometryStorageType::SPATIAL;
}

static bool IsValidOrderedTree(const vector<ColumnDropOwnershipNodeDescriptor> &nodes) {
	if (nodes.empty()) {
		return false;
	}
	vector<idx_t> ancestor_path;
	ancestor_path.reserve(nodes.size());
	vector<idx_t> direct_child_counts(nodes.size(), 0);
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		auto &node = nodes[node_index];
		if (!IsValidLayoutTag(node.layout_tag) || !IsValidChildKey(node.child_key) ||
		    !IsValidStandardGeometryContext(nodes, node_index)) {
			return false;
		}
		if (node_index == 0) {
			if (node.parent_index != ColumnDropOwnershipShape::ROOT_PARENT_INDEX ||
			    node.child_key.role != ColumnDropOwnershipChildRole::ROOT) {
				return false;
			}
			ancestor_path.push_back(node_index);
		} else {
			if (node.parent_index >= node_index || node.child_key.role == ColumnDropOwnershipChildRole::ROOT) {
				return false;
			}
			while (!ancestor_path.empty() && ancestor_path.back() != node.parent_index) {
				ancestor_path.pop_back();
			}
			if (ancestor_path.empty()) {
				return false;
			}
			auto &parent = nodes[node.parent_index];
			auto child_position = direct_child_counts[node.parent_index];
			if (child_position >= ExpectedDirectChildCount(parent) ||
			    !IsExpectedDirectChild(parent, node, child_position) || !ChildTypeMatchesParent(parent, node)) {
				return false;
			}
			direct_child_counts[node.parent_index]++;
			ancestor_path.push_back(node_index);
		}
	}
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		if (direct_child_counts[node_index] != ExpectedDirectChildCount(nodes[node_index])) {
			return false;
		}
	}
	return true;
}

static bool HasUniqueExistingTokens(const vector<ColumnDropOwnershipNodeDescriptor> &nodes) {
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		if (!nodes[node_index].direct_token) {
			continue;
		}
		for (idx_t previous_index = 0; previous_index < node_index; previous_index++) {
			if (nodes[node_index].direct_token == nodes[previous_index].direct_token) {
				return false;
			}
		}
	}
	return true;
}

ColumnDropOwnershipShape::ColumnDropOwnershipShape(vector<ColumnDropOwnershipNodeDescriptor> ordered_nodes_p)
    : ordered_nodes(std::move(ordered_nodes_p)) {
}

ColumnDropOwnershipShape::ColumnDropOwnershipShape(ColumnDropOwnershipShape &&other) noexcept
    : ordered_nodes(std::move(other.ordered_nodes)) {
}

bool ColumnDropOwnershipShape::HasExistingTokens() const noexcept {
	for (auto &node : ordered_nodes) {
		if (node.direct_token) {
			return true;
		}
	}
	return false;
}

void ColumnDropOwnershipShape::InitializeMissingTokens() {
	for (auto &node : ordered_nodes) {
		if (!node.direct_token) {
			node.direct_token = make_shared_ptr<RowGroupColumnDropOwnership>();
		}
	}
}

unique_ptr<ColumnDropOwnershipShape>
ColumnDropOwnershipShape::Capture(vector<ColumnDropOwnershipNodeDescriptor> observed_tree) {
	if (!IsValidOrderedTree(observed_tree) || !HasUniqueExistingTokens(observed_tree)) {
		throw InternalException("Invalid column drop ownership shape");
	}
	ColumnDropOwnershipShape captured(std::move(observed_tree));
	return make_uniq<ColumnDropOwnershipShape>(std::move(captured));
}

static bool IsSpatialStorageRepresentation(const ColumnDropOwnershipNodeDescriptor &node,
                                           const ColumnDropOwnershipNodeDescriptor &parent) {
	return node.layout_tag.logical_type == parent.layout_tag.logical_type ||
	       Geometry::IsSpatialGeometryType(node.layout_tag.logical_type);
}

static bool SpatialStorageTypesMatch(const vector<ColumnDropOwnershipNodeDescriptor> &left_nodes,
                                     const vector<ColumnDropOwnershipNodeDescriptor> &right_nodes, idx_t node_index) {
	if (node_index == 0) {
		return false;
	}
	auto &left = left_nodes[node_index];
	auto &right = right_nodes[node_index];
	if (left.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::STANDARD ||
	    right.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::STANDARD ||
	    left.child_key != ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::GEOMETRY_STORAGE, 0) ||
	    right.child_key != ColumnDropOwnershipChildKey(ColumnDropOwnershipChildRole::GEOMETRY_STORAGE, 0) ||
	    left.parent_index != right.parent_index || left.parent_index >= node_index) {
		return false;
	}
	auto &left_parent = left_nodes[left.parent_index];
	auto &right_parent = right_nodes[right.parent_index];
	if (left_parent.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::GEOMETRY ||
	    right_parent.layout_tag.runtime_kind != ColumnDropOwnershipRuntimeKind::GEOMETRY ||
	    left_parent.layout_tag.layout_value != static_cast<uint64_t>(GeometryStorageType::SPATIAL) ||
	    right_parent.layout_tag.layout_value != static_cast<uint64_t>(GeometryStorageType::SPATIAL)) {
		return false;
	}
	return IsSpatialStorageRepresentation(left, left_parent) && IsSpatialStorageRepresentation(right, right_parent);
}

static bool ShapesMatch(const ColumnDropOwnershipShape &left, const ColumnDropOwnershipShape &right) {
	if (left.NodeCount() != right.NodeCount()) {
		return false;
	}
	auto &left_nodes = left.GetNodes();
	auto &right_nodes = right.GetNodes();
	for (idx_t node_index = 0; node_index < left_nodes.size(); node_index++) {
		auto &left_node = left_nodes[node_index];
		auto &right_node = right_nodes[node_index];
		auto logical_types_match = left_node.layout_tag.logical_type == right_node.layout_tag.logical_type ||
		                           SpatialStorageTypesMatch(left_nodes, right_nodes, node_index);
		if (left_node.layout_tag.runtime_kind != right_node.layout_tag.runtime_kind || !logical_types_match ||
		    left_node.layout_tag.layout_value != right_node.layout_tag.layout_value ||
		    left_node.child_key != right_node.child_key || left_node.parent_index != right_node.parent_index) {
			return false;
		}
	}
	return true;
}

static void CopyTokenPlan(const ColumnDropOwnershipShape &shape,
                          vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) noexcept {
	for (idx_t node_index = 0; node_index < shape.NodeCount(); node_index++) {
		canonical_tokens[node_index] = shape.GetNodes()[node_index].direct_token;
	}
}

void ColumnDropOwnershipBundle::Initialize(unique_ptr<ColumnDropOwnershipShape> candidate,
                                           vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) {
	if (!candidate) {
		throw InternalException("Cannot initialize column drop ownership bundle from a null shape");
	}
	if (canonical_tokens.size() != candidate->NodeCount()) {
		throw InternalException("Column drop ownership initialization plan has the wrong size");
	}
	lock_guard<mutex> guard(bind_lock);
	if (bound_shape) {
		throw InternalException("Column drop ownership bundle is already initialized");
	}
	candidate->InitializeMissingTokens();
	CopyTokenPlan(*candidate, canonical_tokens);
	bound_shape = std::move(candidate);
}

ColumnDropOwnershipBindResult
ColumnDropOwnershipBundle::Bind(unique_ptr<ColumnDropOwnershipShape> candidate,
                                vector<shared_ptr<RowGroupColumnDropOwnership>> &canonical_tokens) noexcept {
	try {
		if (!candidate || canonical_tokens.size() != candidate->NodeCount()) {
			return ColumnDropOwnershipBindResult::MISMATCH;
		}
		lock_guard<mutex> guard(bind_lock);
		if (!bound_shape) {
			if (candidate->HasExistingTokens()) {
				return ColumnDropOwnershipBindResult::MISMATCH;
			}
			candidate->InitializeMissingTokens();
			CopyTokenPlan(*candidate, canonical_tokens);
			bound_shape = std::move(candidate);
			return ColumnDropOwnershipBindResult::ADOPTED;
		}
		if (!ShapesMatch(*bound_shape, *candidate)) {
			return ColumnDropOwnershipBindResult::MISMATCH;
		}
		for (idx_t node_index = 0; node_index < bound_shape->NodeCount(); node_index++) {
			if (candidate->ordered_nodes[node_index].direct_token &&
			    candidate->ordered_nodes[node_index].direct_token !=
			        bound_shape->ordered_nodes[node_index].direct_token) {
				return ColumnDropOwnershipBindResult::MISMATCH;
			}
		}
		CopyTokenPlan(*bound_shape, canonical_tokens);
		return ColumnDropOwnershipBindResult::VERIFIED;
	} catch (...) { // NOLINT: this is a no-throw publication mapping
		return ColumnDropOwnershipBindResult::MISMATCH;
	}
}

} // namespace duckdb
