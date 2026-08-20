#include "catch.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/recluster/replacement_manifest.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

static RowGroupPhysicalIdentity ManifestOldGroup(row_t start, idx_t count, block_id_t metadata_block) {
	RowGroupPhysicalIdentity result;
	result.start = start;
	result.count = count;
	result.sealed = true;
	result.columns.push_back({11, LogicalType::INTEGER, MetaBlockPointer(metadata_block, 0), {}});
	result.immutable_data_checksum = ComputeRowGroupPhysicalIdentityChecksumV1(result);
	return result;
}

static RowGroupPointer ManifestReplacementGroup(idx_t start, idx_t count, block_id_t metadata_block) {
	RowGroupPointer result;
	result.row_start = start;
	result.tuple_count = count;
	result.data_pointers.emplace_back(metadata_block, 0);
	result.has_per_column_metadata_blocks = true;
	result.sort_metadata = {7, 9};
	return result;
}

static ReplacementManifest CreateManifest() {
	ReplacementManifest result;
	result.header.task_id = hugeint_t(1, 2);
	result.header.table_id = hugeint_t(3, 4);
	result.header.prepared_layout_version = 5;
	result.header.sort_order_id = 7;
	result.header.run_id = 9;
	result.header.input_range = {2048, 6144};
	result.header.last_applied_delete_sequence = 12;
	result.header.manifest_revision = 3;
	result.sort_columns.push_back({11, OrderType::ASCENDING, OrderByNullType::NULLS_LAST});
	result.physical_columns.push_back({11, LogicalType::INTEGER});
	result.old_groups.push_back(ManifestOldGroup(2048, 2048, 20));
	result.old_groups.push_back(ManifestOldGroup(4096, 2048, 21));
	result.replacement_groups.push_back(ManifestReplacementGroup(2048, 2048, 30));
	result.replacement_groups.push_back(ManifestReplacementGroup(4096, 1024, 31));
	result.replacement_groups[0].per_column_metadata_blocks.AddColumn(0, {(idx_t(2) << 56ULL) | 40});
	result.all_referenced_blocks = {30, 31, 40};
	result.Seal();
	return result;
}

static ReplacementManifest CreateTwoColumnManifest() {
	auto result = CreateManifest();
	result.physical_columns.push_back({12, LogicalType::VARCHAR});
	for (idx_t group_index = 0; group_index < result.old_groups.size(); group_index++) {
		auto &old_group = result.old_groups[group_index];
		old_group.columns.push_back({12, LogicalType::VARCHAR, MetaBlockPointer(22 + group_index, 0), {}});
		old_group.immutable_data_checksum = ComputeRowGroupPhysicalIdentityChecksumV1(old_group);
	}
	for (auto &replacement_group : result.replacement_groups) {
		replacement_group.data_pointers.push_back(replacement_group.data_pointers[0]);
	}
	result.Seal();
	return result;
}

TEST_CASE("Replacement manifest v1 round-trips its recovery contract", "[storage][replacement_manifest]") {
	auto manifest = CreateManifest();
	MemoryStream stream;
	manifest.Write(stream);
	REQUIRE(stream.GetPosition() == manifest.payload_size + 24);
	stream.SetPosition(0);
	auto loaded = ReplacementManifest::Read(stream);

	REQUIRE(loaded.header.format_version == REPLACEMENT_MANIFEST_FORMAT_VERSION);
	REQUIRE(loaded.header.task_id == manifest.header.task_id);
	REQUIRE(loaded.header.table_id == manifest.header.table_id);
	REQUIRE(loaded.header.prepared_layout_version == 5);
	REQUIRE(loaded.header.sort_order_id == 7);
	REQUIRE(loaded.header.run_id == 9);
	REQUIRE(loaded.header.input_range.start == 2048);
	REQUIRE(loaded.header.input_range.end == 6144);
	REQUIRE(loaded.header.last_applied_delete_sequence == 12);
	REQUIRE(loaded.header.manifest_revision == 3);
	REQUIRE(loaded.sort_columns == manifest.sort_columns);
	REQUIRE(loaded.physical_columns == manifest.physical_columns);
	REQUIRE(loaded.old_groups == manifest.old_groups);
	REQUIRE(loaded.replacement_groups.size() == 2);
	REQUIRE(loaded.replacement_groups[0].row_start == 2048);
	REQUIRE(loaded.replacement_groups[0].tuple_count == 2048);
	REQUIRE(loaded.replacement_groups[1].row_start == 4096);
	REQUIRE(loaded.replacement_groups[1].tuple_count == 1024);
	REQUIRE(loaded.replacement_groups[1].sort_metadata == RowGroupSortMetadata {7, 9});
	REQUIRE(loaded.all_referenced_blocks == manifest.all_referenced_blocks);
	REQUIRE(loaded.payload_size == manifest.payload_size);
	REQUIRE(loaded.checksum == manifest.checksum);
}

TEST_CASE("Replacement manifest rejects corrupt or inconsistent state", "[storage][replacement_manifest]") {
	auto manifest = CreateManifest();
	MemoryStream stream;
	manifest.Write(stream);
	auto serialized_size = stream.GetPosition();
	stream.GetData()[serialized_size - 1] ^= 0xff;
	stream.SetPosition(0);
	REQUIRE_THROWS_AS(ReplacementManifest::Read(stream), SerializationException);

	auto invalid = CreateManifest();
	invalid.all_referenced_blocks = {30, 30};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.replacement_groups[1].row_start++;
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.all_referenced_blocks = {31, 40};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.replacement_groups[0].per_column_metadata_blocks.AddColumn(1, {40});
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.old_groups[0] = ManifestOldGroup(2048, 1024, 20);
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	auto invalid_metadata_pointer = (idx_t(MetadataManager::METADATA_BLOCK_COUNT) << 56ULL) | 30;
	invalid = CreateManifest();
	invalid.replacement_groups[0].data_pointers[0] = MetaBlockPointer(invalid_metadata_pointer, 0);
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.replacement_groups[0].deletes_pointers.emplace_back(invalid_metadata_pointer, 0);
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	PerColumnMetadataBlock marker;
	marker.is_column_index = true;
	marker.index = 0;
	PerColumnMetadataBlock block;
	block.is_column_index = false;
	block.index = 40;

	invalid = CreateManifest();
	invalid.replacement_groups[0].per_column_metadata_blocks.data = {block};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.replacement_groups[0].per_column_metadata_blocks.data = {marker, block, marker, block};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateTwoColumnManifest();
	auto second_marker = marker;
	second_marker.index = 1;
	invalid.replacement_groups[0].per_column_metadata_blocks.data = {second_marker, block, marker, block};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	invalid = CreateManifest();
	invalid.replacement_groups[0].per_column_metadata_blocks.data = {marker};
	REQUIRE_THROWS_AS(invalid.Seal(), SerializationException);

	auto empty = CreateManifest();
	empty.replacement_groups.clear();
	empty.all_referenced_blocks.clear();
	REQUIRE_NOTHROW(empty.Seal());
	empty.all_referenced_blocks.push_back(40);
	REQUIRE_THROWS_AS(empty.Seal(), SerializationException);
}
