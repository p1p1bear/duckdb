#include "catch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/storage/table/column_drop_ownership_bundle.hpp"

#include <atomic>
#include <thread>
#include <type_traits>
#include <utility>

using namespace duckdb; // NOLINT

using BundleTestShape = ColumnDropOwnershipShape;
using BundleTestBundle = ColumnDropOwnershipBundle;
using BundleTestBindResult = ColumnDropOwnershipBindResult;
using BundleTestLayoutTag = ColumnDropOwnershipLayoutTag;
using BundleTestDescriptor = ColumnDropOwnershipNodeDescriptor;
using BundleTestDescriptors = duckdb::vector<BundleTestDescriptor>;
using BundleTestToken = RowGroupColumnDropOwnership;
using BundleTestTokenPtr = duckdb::shared_ptr<BundleTestToken>;
using BundleTestTokenPlan = duckdb::vector<BundleTestTokenPtr>;

static_assert(!std::is_copy_constructible<BundleTestShape>::value, "ownership shapes must be immutable");
static_assert(!std::is_copy_constructible<BundleTestBundle>::value, "ownership bundles must have stable identity");
static_assert(!std::is_constructible<BundleTestLayoutTag, ColumnDropOwnershipRuntimeKind, LogicalType>::value,
              "layout observers must provide an explicit runtime discriminator");
static_assert(noexcept(std::declval<BundleTestBundle &>().Bind(std::declval<duckdb::unique_ptr<BundleTestShape>>(),
                                                               std::declval<BundleTestTokenPlan &>())),
              "ownership bundle binding must be noexcept");

static BundleTestTokenPtr MakeBundleTestToken() {
	return make_shared_ptr<BundleTestToken>();
}

static BundleTestDescriptor MakeBundleTestNode(ColumnDropOwnershipRuntimeKind kind, LogicalType type,
                                               ColumnDropOwnershipChildRole role, idx_t ordinal, idx_t parent_index,
                                               BundleTestTokenPtr token = nullptr, uint64_t layout_value = 0) {
	return BundleTestDescriptor(ColumnDropOwnershipLayoutTag(kind, std::move(type), layout_value),
	                            ColumnDropOwnershipChildKey(role, ordinal), parent_index, std::move(token));
}

static BundleTestDescriptors MakeBundleTestListTree(BundleTestTokenPtr root = nullptr,
                                                    BundleTestTokenPtr validity = nullptr,
                                                    BundleTestTokenPtr element = nullptr,
                                                    BundleTestTokenPtr element_validity = nullptr,
                                                    LogicalType element_type = LogicalType::DECIMAL(10, 2)) {
	BundleTestDescriptors result;
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::LIST, LogicalType::LIST(element_type),
	                                    ColumnDropOwnershipChildRole::ROOT, 0, BundleTestShape::ROOT_PARENT_INDEX,
	                                    std::move(root)));
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY, LogicalType(LogicalTypeId::VALIDITY),
	                                    ColumnDropOwnershipChildRole::VALIDITY, 0, 0, std::move(validity)));
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::STANDARD, element_type,
	                                    ColumnDropOwnershipChildRole::ELEMENT, 0, 0, std::move(element)));
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY, LogicalType(LogicalTypeId::VALIDITY),
	                                    ColumnDropOwnershipChildRole::VALIDITY, 0, 2, std::move(element_validity)));
	return result;
}

static ColumnDropOwnershipRuntimeKind BundleTestPhysicalKind(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VALIDITY) {
		return ColumnDropOwnershipRuntimeKind::VALIDITY;
	}
	switch (type.InternalType()) {
	case PhysicalType::LIST:
		return ColumnDropOwnershipRuntimeKind::LIST;
	case PhysicalType::STRUCT:
		return ColumnDropOwnershipRuntimeKind::STRUCT;
	case PhysicalType::ARRAY:
		return ColumnDropOwnershipRuntimeKind::ARRAY;
	default:
		return ColumnDropOwnershipRuntimeKind::STANDARD;
	}
}

static idx_t AppendBundleTestPhysicalTree(BundleTestDescriptors &result, const LogicalType &type,
                                          ColumnDropOwnershipChildRole role, idx_t ordinal, idx_t parent_index) {
	auto node_index = result.size();
	auto runtime_kind = BundleTestPhysicalKind(type);
	result.push_back(MakeBundleTestNode(runtime_kind, type, role, ordinal, parent_index));
	switch (runtime_kind) {
	case ColumnDropOwnershipRuntimeKind::STANDARD:
		AppendBundleTestPhysicalTree(result, LogicalType(LogicalTypeId::VALIDITY),
		                             ColumnDropOwnershipChildRole::VALIDITY, 0, node_index);
		break;
	case ColumnDropOwnershipRuntimeKind::LIST:
		AppendBundleTestPhysicalTree(result, LogicalType(LogicalTypeId::VALIDITY),
		                             ColumnDropOwnershipChildRole::VALIDITY, 0, node_index);
		AppendBundleTestPhysicalTree(result, ListType::GetChildType(type), ColumnDropOwnershipChildRole::ELEMENT, 0,
		                             node_index);
		break;
	case ColumnDropOwnershipRuntimeKind::STRUCT: {
		AppendBundleTestPhysicalTree(result, LogicalType(LogicalTypeId::VALIDITY),
		                             ColumnDropOwnershipChildRole::VALIDITY, 0, node_index);
		auto &children = StructType::GetChildTypes(type);
		for (idx_t child_index = 0; child_index < children.size(); child_index++) {
			AppendBundleTestPhysicalTree(result, children[child_index].second,
			                             ColumnDropOwnershipChildRole::STRUCT_FIELD, child_index, node_index);
		}
		break;
	}
	case ColumnDropOwnershipRuntimeKind::ARRAY:
		AppendBundleTestPhysicalTree(result, LogicalType(LogicalTypeId::VALIDITY),
		                             ColumnDropOwnershipChildRole::VALIDITY, 0, node_index);
		AppendBundleTestPhysicalTree(result, ArrayType::GetChildType(type), ColumnDropOwnershipChildRole::ELEMENT, 0,
		                             node_index);
		break;
	case ColumnDropOwnershipRuntimeKind::VALIDITY:
		break;
	case ColumnDropOwnershipRuntimeKind::VARIANT:
	case ColumnDropOwnershipRuntimeKind::GEOMETRY:
	case ColumnDropOwnershipRuntimeKind::INVALID:
		throw InternalException("Unexpected special runtime kind in physical test tree");
	}
	return node_index;
}

static BundleTestDescriptors MakeBundleTestGeometryTree(GeometryStorageType storage_type,
                                                        bool use_spatial_alias = false) {
	BundleTestDescriptors result;
	auto geometry_type = LogicalType::GEOMETRY();
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::GEOMETRY, geometry_type,
	                                    ColumnDropOwnershipChildRole::ROOT, 0, BundleTestShape::ROOT_PARENT_INDEX,
	                                    nullptr, static_cast<uint64_t>(storage_type)));
	auto storage_layout = use_spatial_alias ? Geometry::GetSpatialGeometryType()
	                      : storage_type == GeometryStorageType::WKB || storage_type == GeometryStorageType::SPATIAL
	                          ? geometry_type
	                          : Geometry::GetVectorizedType(storage_type);
	AppendBundleTestPhysicalTree(result, storage_layout, ColumnDropOwnershipChildRole::GEOMETRY_STORAGE, 0, 0);
	return result;
}

static BundleTestDescriptors MakeBundleTestVariantTree(bool shredded) {
	BundleTestDescriptors result;
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VARIANT, LogicalType::VARIANT(),
	                                    ColumnDropOwnershipChildRole::ROOT, 0, BundleTestShape::ROOT_PARENT_INDEX,
	                                    nullptr, shredded ? 1 : 0));
	result.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY, LogicalType(LogicalTypeId::VALIDITY),
	                                    ColumnDropOwnershipChildRole::VALIDITY, 0, 0));
	AppendBundleTestPhysicalTree(result, VariantShredding::GetUnshreddedType(),
	                             ColumnDropOwnershipChildRole::VARIANT_CHILD, 0, 0);
	if (shredded) {
		AppendBundleTestPhysicalTree(result, LogicalType::BIGINT, ColumnDropOwnershipChildRole::VARIANT_CHILD, 1, 0);
	}
	return result;
}

TEST_CASE("Column drop ownership shape captures existing tokens without allocating missing tokens",
          "[storage][drop_ownership_bundle]") {
	auto root = MakeBundleTestToken();
	auto element = MakeBundleTestToken();
	auto shape = BundleTestShape::Capture(MakeBundleTestListTree(root, nullptr, element));

	REQUIRE(shape->NodeCount() == 4);
	auto &nodes = shape->GetNodes();
	REQUIRE(nodes[0].direct_token == root);
	REQUIRE(nodes[2].direct_token == element);
	REQUIRE(!nodes[1].direct_token);
	REQUIRE(!nodes[3].direct_token);
	REQUIRE(nodes[0].layout_tag.logical_type == LogicalType::LIST(LogicalType::DECIMAL(10, 2)));
}

TEST_CASE("Column drop ownership shape rejects invalid typed layouts", "[storage][drop_ownership_bundle]") {
	BundleTestDescriptors empty;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(empty)), InternalException);

	auto wrong_runtime = MakeBundleTestListTree();
	wrong_runtime[0].layout_tag.runtime_kind = ColumnDropOwnershipRuntimeKind::STANDARD;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(wrong_runtime)), InternalException);

	BundleTestDescriptors standard_geometry_root;
	AppendBundleTestPhysicalTree(standard_geometry_root, LogicalType::GEOMETRY(), ColumnDropOwnershipChildRole::ROOT, 0,
	                             BundleTestShape::ROOT_PARENT_INDEX);
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(standard_geometry_root)), InternalException);

	BundleTestDescriptors standard_geometry_field;
	auto geometry_struct = LogicalType::STRUCT({{"geometry", LogicalType::GEOMETRY()}});
	AppendBundleTestPhysicalTree(standard_geometry_field, geometry_struct, ColumnDropOwnershipChildRole::ROOT, 0,
	                             BundleTestShape::ROOT_PARENT_INDEX);
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(standard_geometry_field)), InternalException);

	auto wrong_ordinal = MakeBundleTestListTree();
	wrong_ordinal[2].child_key.ordinal = 1;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(wrong_ordinal)), InternalException);

	auto wrong_child_type = MakeBundleTestListTree();
	wrong_child_type[2].layout_tag.logical_type = LogicalType::DECIMAL(12, 2);
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(wrong_child_type)), InternalException);

	auto duplicate_child = MakeBundleTestListTree();
	duplicate_child[2].child_key = duplicate_child[1].child_key;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(duplicate_child)), InternalException);

	auto duplicate_token = MakeBundleTestToken();
	auto duplicate_existing_token = MakeBundleTestListTree(duplicate_token, duplicate_token);
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(duplicate_existing_token)), InternalException);

	auto invalid_plain_layout_value = MakeBundleTestListTree();
	invalid_plain_layout_value[0].layout_tag.layout_value = 1;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(invalid_plain_layout_value)), InternalException);

	auto invalid_variant_layout_value = MakeBundleTestVariantTree(false);
	invalid_variant_layout_value[0].layout_tag.layout_value = 2;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(invalid_variant_layout_value)), InternalException);

	auto wrong_variant_unshredded_type = MakeBundleTestVariantTree(false);
	auto wrong_unshredded_children = StructType::GetChildTypes(VariantShredding::GetUnshreddedType());
	wrong_unshredded_children[0].first = "wrong_keys";
	wrong_variant_unshredded_type[2].layout_tag.logical_type =
	    LogicalType::STRUCT(std::move(wrong_unshredded_children));
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(wrong_variant_unshredded_type)), InternalException);

	auto incomplete = MakeBundleTestListTree();
	incomplete.pop_back();
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(incomplete)), InternalException);

	BundleTestDescriptors wrong_child_order;
	auto element_type = LogicalType::DECIMAL(10, 2);
	wrong_child_order.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::LIST,
	                                               LogicalType::LIST(element_type), ColumnDropOwnershipChildRole::ROOT,
	                                               0, BundleTestShape::ROOT_PARENT_INDEX));
	wrong_child_order.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::STANDARD, element_type,
	                                               ColumnDropOwnershipChildRole::ELEMENT, 0, 0));
	wrong_child_order.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY,
	                                               LogicalType(LogicalTypeId::VALIDITY),
	                                               ColumnDropOwnershipChildRole::VALIDITY, 0, 1));
	wrong_child_order.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY,
	                                               LogicalType(LogicalTypeId::VALIDITY),
	                                               ColumnDropOwnershipChildRole::VALIDITY, 0, 0));
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(wrong_child_order)), InternalException);

	child_list_t<LogicalType> struct_fields;
	struct_fields.emplace_back("a", LogicalType::INTEGER);
	struct_fields.emplace_back("b", LogicalType::INTEGER);
	BundleTestDescriptors not_preorder;
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::STRUCT,
	                                          LogicalType::STRUCT(struct_fields), ColumnDropOwnershipChildRole::ROOT, 0,
	                                          BundleTestShape::ROOT_PARENT_INDEX));
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY,
	                                          LogicalType(LogicalTypeId::VALIDITY),
	                                          ColumnDropOwnershipChildRole::VALIDITY, 0, 0));
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::STANDARD, LogicalType::INTEGER,
	                                          ColumnDropOwnershipChildRole::STRUCT_FIELD, 0, 0));
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::STANDARD, LogicalType::INTEGER,
	                                          ColumnDropOwnershipChildRole::STRUCT_FIELD, 1, 0));
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY,
	                                          LogicalType(LogicalTypeId::VALIDITY),
	                                          ColumnDropOwnershipChildRole::VALIDITY, 0, 2));
	not_preorder.push_back(MakeBundleTestNode(ColumnDropOwnershipRuntimeKind::VALIDITY,
	                                          LogicalType(LogicalTypeId::VALIDITY),
	                                          ColumnDropOwnershipChildRole::VALIDITY, 0, 3));
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(not_preorder)), InternalException);
}

TEST_CASE("Column drop ownership shape accepts variant and geometry runtime layouts",
          "[storage][drop_ownership_bundle]") {
	REQUIRE_NOTHROW(BundleTestShape::Capture(MakeBundleTestVariantTree(false)));
	REQUIRE_NOTHROW(BundleTestShape::Capture(MakeBundleTestVariantTree(true)));
	REQUIRE_NOTHROW(BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::WKB)));
	REQUIRE_NOTHROW(BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::SPATIAL)));
	REQUIRE_NOTHROW(BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::SPATIAL, true)));
	auto point_shape = BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::POINT_XY));
	REQUIRE(point_shape->GetNodes()[1].layout_tag.logical_type ==
	        Geometry::GetVectorizedType(GeometryStorageType::POINT_XY));

	auto invalid_wkb_alias = MakeBundleTestGeometryTree(GeometryStorageType::WKB, true);
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(invalid_wkb_alias)), InternalException);

	auto invalid_geometry = MakeBundleTestGeometryTree(GeometryStorageType::WKB);
	invalid_geometry[0].layout_tag.layout_value = 2;
	REQUIRE_THROWS_AS(BundleTestShape::Capture(std::move(invalid_geometry)), InternalException);
}

TEST_CASE("Column drop ownership bundle verifies and maps an exact observed shape",
          "[storage][drop_ownership_bundle]") {
	auto canonical_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	auto sentinel = MakeBundleTestToken();
	BundleTestTokenPlan plan(canonical_shape->NodeCount(), sentinel);
	BundleTestBundle bundle;

	REQUIRE(bundle.Bind(std::move(canonical_shape), plan) == BundleTestBindResult::ADOPTED);
	auto canonical_plan = plan;

	auto observed_shape =
	    BundleTestShape::Capture(MakeBundleTestListTree(canonical_plan[0], nullptr, canonical_plan[2]));
	REQUIRE(bundle.Bind(std::move(observed_shape), plan) == BundleTestBindResult::VERIFIED);
	REQUIRE(plan == canonical_plan);
}

TEST_CASE("Mixed existing ownership requires explicit bundle initialization", "[storage][drop_ownership_bundle]") {
	auto existing_root = MakeBundleTestToken();
	auto mismatched_shape = BundleTestShape::Capture(MakeBundleTestListTree(existing_root));
	auto sentinel = MakeBundleTestToken();
	BundleTestTokenPlan plan(mismatched_shape->NodeCount(), sentinel);
	BundleTestBundle bundle;

	REQUIRE(bundle.Bind(std::move(mismatched_shape), plan) == BundleTestBindResult::MISMATCH);
	for (auto &token : plan) {
		REQUIRE(token == sentinel);
	}

	auto existing_shape = BundleTestShape::Capture(MakeBundleTestListTree(existing_root));
	REQUIRE_NOTHROW(bundle.Initialize(std::move(existing_shape), plan));
	REQUIRE(plan[0] == existing_root);
	for (auto &token : plan) {
		REQUIRE(token);
	}

	auto lazy_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestTokenPlan lazy_plan(lazy_shape->NodeCount(), sentinel);
	REQUIRE(bundle.Bind(std::move(lazy_shape), lazy_plan) == BundleTestBindResult::VERIFIED);
	for (idx_t node_index = 0; node_index < lazy_plan.size(); node_index++) {
		REQUIRE(lazy_plan[node_index] == plan[node_index]);
	}

	auto second_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestTokenPlan already_bound_plan(second_shape->NodeCount(), sentinel);
	REQUIRE_THROWS_AS(bundle.Initialize(std::move(second_shape), already_bound_plan), InternalException);
	for (auto &token : already_bound_plan) {
		REQUIRE(token == sentinel);
	}

	BundleTestBundle lazy_first_bundle;
	auto lazy_first_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestTokenPlan lazy_first_plan(lazy_first_shape->NodeCount());
	REQUIRE(lazy_first_bundle.Bind(std::move(lazy_first_shape), lazy_first_plan) == BundleTestBindResult::ADOPTED);
	auto late_existing_shape = BundleTestShape::Capture(MakeBundleTestListTree(MakeBundleTestToken()));
	BundleTestTokenPlan late_initialize_plan(late_existing_shape->NodeCount(), sentinel);
	REQUIRE_THROWS_AS(lazy_first_bundle.Initialize(std::move(late_existing_shape), late_initialize_plan),
	                  InternalException);
	for (auto &token : late_initialize_plan) {
		REQUIRE(token == sentinel);
	}
}

TEST_CASE("Explicit initialization races safely with lazy first binding", "[storage][drop_ownership_bundle]") {
	auto existing_root = MakeBundleTestToken();
	auto initialized_candidate = BundleTestShape::Capture(MakeBundleTestListTree(existing_root));
	auto lazy_candidate = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestBundle bundle;
	auto initialize_sentinel = MakeBundleTestToken();
	BundleTestTokenPlan initialize_plan(initialized_candidate->NodeCount(), initialize_sentinel);
	BundleTestTokenPlan lazy_plan(lazy_candidate->NodeCount());
	bool initialize_succeeded = false;
	bool initialize_failed = false;
	BundleTestBindResult bind_result = BundleTestBindResult::MISMATCH;
	std::atomic<bool> start(false);

	std::thread initializer([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		try {
			bundle.Initialize(std::move(initialized_candidate), initialize_plan);
			initialize_succeeded = true;
		} catch (...) {
			initialize_failed = true;
		}
	});
	std::thread binder([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		bind_result = bundle.Bind(std::move(lazy_candidate), lazy_plan);
	});
	start = true;
	initializer.join();
	binder.join();

	REQUIRE(initialize_succeeded != initialize_failed);
	if (initialize_succeeded) {
		REQUIRE(bind_result == BundleTestBindResult::VERIFIED);
		REQUIRE(initialize_plan[0] == existing_root);
		for (idx_t node_index = 0; node_index < lazy_plan.size(); node_index++) {
			REQUIRE(lazy_plan[node_index] == initialize_plan[node_index]);
		}
	} else {
		REQUIRE(bind_result == BundleTestBindResult::ADOPTED);
		REQUIRE(lazy_plan[0] != existing_root);
		for (auto &token : initialize_plan) {
			REQUIRE(token == initialize_sentinel);
		}
	}
}

TEST_CASE("Column drop ownership bundle rejects conflicting existing tokens without changing its plan",
          "[storage][drop_ownership_bundle]") {
	auto canonical_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestBundle bundle;
	auto node_count = canonical_shape->NodeCount();
	BundleTestTokenPlan initial_plan(node_count);
	REQUIRE(bundle.Bind(std::move(canonical_shape), initial_plan) == BundleTestBindResult::ADOPTED);

	auto foreign = MakeBundleTestToken();
	auto conflicting_shape = BundleTestShape::Capture(MakeBundleTestListTree(foreign));
	auto sentinel = MakeBundleTestToken();
	BundleTestTokenPlan unchanged_plan(node_count, sentinel);
	REQUIRE(bundle.Bind(std::move(conflicting_shape), unchanged_plan) == BundleTestBindResult::MISMATCH);
	for (auto &token : unchanged_plan) {
		REQUIRE(token == sentinel);
	}

	auto wrong_type_shape = BundleTestShape::Capture(
	    MakeBundleTestListTree(nullptr, nullptr, nullptr, nullptr, LogicalType::DECIMAL(12, 2)));
	REQUIRE(bundle.Bind(std::move(wrong_type_shape), unchanged_plan) == BundleTestBindResult::MISMATCH);
	for (auto &token : unchanged_plan) {
		REQUIRE(token == sentinel);
	}

	BundleTestTokenPlan wrong_size(1, sentinel);
	auto same_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	REQUIRE(bundle.Bind(std::move(same_shape), wrong_size) == BundleTestBindResult::MISMATCH);
	REQUIRE(wrong_size[0] == sentinel);
}

TEST_CASE("Column drop ownership bundle compares the exact runtime layout value", "[storage][drop_ownership_bundle]") {
	auto canonical_shape = BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::WKB));
	BundleTestBundle bundle;
	BundleTestTokenPlan canonical_plan(canonical_shape->NodeCount());
	REQUIRE(bundle.Bind(std::move(canonical_shape), canonical_plan) == BundleTestBindResult::ADOPTED);

	auto different_layout = BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::SPATIAL));
	auto sentinel = MakeBundleTestToken();
	BundleTestTokenPlan unchanged_plan(different_layout->NodeCount(), sentinel);
	REQUIRE(bundle.Bind(std::move(different_layout), unchanged_plan) == BundleTestBindResult::MISMATCH);
	for (auto &token : unchanged_plan) {
		REQUIRE(token == sentinel);
	}
}

TEST_CASE("Spatial geometry reload preserves canonical ownership tokens", "[storage][drop_ownership_bundle]") {
	auto checkpoint_shape = BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::SPATIAL, true));
	BundleTestBundle bundle;
	BundleTestTokenPlan checkpoint_plan(checkpoint_shape->NodeCount());
	REQUIRE_NOTHROW(bundle.Initialize(std::move(checkpoint_shape), checkpoint_plan));

	auto restart_shape = BundleTestShape::Capture(MakeBundleTestGeometryTree(GeometryStorageType::SPATIAL));
	BundleTestTokenPlan restart_plan(restart_shape->NodeCount());
	REQUIRE(bundle.Bind(std::move(restart_shape), restart_plan) == BundleTestBindResult::VERIFIED);
	REQUIRE(restart_plan.size() == checkpoint_plan.size());
	for (idx_t node_index = 0; node_index < restart_plan.size(); node_index++) {
		REQUIRE(restart_plan[node_index] == checkpoint_plan[node_index]);
	}
}

TEST_CASE("Concurrent column drop ownership binding adopts only one candidate", "[storage][drop_ownership_bundle]") {
	auto first_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	auto second_shape = BundleTestShape::Capture(MakeBundleTestListTree());
	BundleTestBundle bundle;
	BundleTestTokenPlan first_plan(first_shape->NodeCount());
	BundleTestTokenPlan second_plan(second_shape->NodeCount());
	BundleTestBindResult first_result = BundleTestBindResult::MISMATCH;
	BundleTestBindResult second_result = BundleTestBindResult::MISMATCH;
	std::atomic<bool> start(false);

	std::thread first([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		first_result = bundle.Bind(std::move(first_shape), first_plan);
	});
	std::thread second([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		second_result = bundle.Bind(std::move(second_shape), second_plan);
	});
	start = true;
	first.join();
	second.join();

	auto adopted_count = (first_result == BundleTestBindResult::ADOPTED ? 1 : 0) +
	                     (second_result == BundleTestBindResult::ADOPTED ? 1 : 0);
	auto mismatch_count = (first_result == BundleTestBindResult::MISMATCH ? 1 : 0) +
	                      (second_result == BundleTestBindResult::MISMATCH ? 1 : 0);
	auto verified_count = (first_result == BundleTestBindResult::VERIFIED ? 1 : 0) +
	                      (second_result == BundleTestBindResult::VERIFIED ? 1 : 0);
	REQUIRE(adopted_count == 1);
	REQUIRE(verified_count == 1);
	REQUIRE(mismatch_count == 0);
	for (idx_t node_index = 0; node_index < first_plan.size(); node_index++) {
		REQUIRE(first_plan[node_index] == second_plan[node_index]);
	}
}
