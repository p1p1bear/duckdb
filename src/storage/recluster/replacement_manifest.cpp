#include "duckdb/storage/recluster/replacement_manifest.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/storage_compatibility.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {

static constexpr char REPLACEMENT_MANIFEST_MAGIC[] = {'R', 'C', 'L', '1'};
static constexpr idx_t REPLACEMENT_MANIFEST_MAX_PAYLOAD_SIZE = 64ULL * 1024ULL * 1024ULL;

void ManifestColumnDefinition::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<persistent_column_id_t>(100, "column_id", column_id);
	serializer.WriteProperty<LogicalType>(101, "type", type);
}

ManifestColumnDefinition ManifestColumnDefinition::Deserialize(Deserializer &deserializer) {
	ManifestColumnDefinition result;
	deserializer.ReadProperty<persistent_column_id_t>(100, "column_id", result.column_id);
	deserializer.ReadProperty<LogicalType>(101, "type", result.type);
	return result;
}

bool ManifestColumnDefinition::operator==(const ManifestColumnDefinition &other) const {
	return column_id == other.column_id && type == other.type;
}

static SerializationOptions ReplacementManifestSerializationOptions() {
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V2_0_0);
	return options;
}

vector<uint8_t> ReplacementManifest::SerializePayload() const {
	MemoryStream stream;
	BinarySerializer serializer(stream, ReplacementManifestSerializationOptions());
	serializer.Begin();
	serializer.WriteProperty<recluster_task_id_t>(100, "task_id", header.task_id);
	serializer.WriteProperty<persistent_table_id_t>(101, "table_id", header.table_id);
	serializer.WriteProperty<layout_version_t>(102, "prepared_layout_version", header.prepared_layout_version);
	serializer.WriteProperty<sort_order_id_t>(103, "sort_order_id", header.sort_order_id);
	serializer.WriteProperty<sort_run_id_t>(104, "run_id", header.run_id);
	serializer.WriteProperty<row_t>(105, "range_start", header.input_range.start);
	serializer.WriteProperty<row_t>(106, "range_end", header.input_range.end);
	serializer.WriteProperty<delete_sequence_t>(107, "last_applied_delete_sequence",
	                                            header.last_applied_delete_sequence);
	serializer.WriteProperty<uint64_t>(108, "manifest_revision", header.manifest_revision);
	serializer.WriteProperty<vector<SortColumnDefinition>>(109, "sort_columns", sort_columns);
	serializer.WriteProperty<vector<ManifestColumnDefinition>>(110, "physical_columns", physical_columns);
	serializer.WriteProperty<vector<RowGroupPhysicalIdentity>>(111, "old_groups", old_groups);
	serializer.WriteList(
	    112, "replacement_groups", replacement_groups.size(), [&](Serializer::List &list, idx_t index) {
		    list.WriteObject([&](Serializer &object) { RowGroup::Serialize(replacement_groups[index], object, true); });
	    });
	serializer.WriteProperty<vector<block_id_t>>(113, "all_referenced_blocks", all_referenced_blocks);
	serializer.End();
	return vector<uint8_t>(stream.GetData(), stream.GetData() + stream.GetPosition());
}

ReplacementManifest ReplacementManifest::DeserializePayload(data_ptr_t payload, idx_t payload_size_p) {
	MemoryStream stream(payload, payload_size_p);
	BinaryDeserializer deserializer(stream);
	ReplacementManifest result;
	deserializer.Begin();
	deserializer.ReadProperty<recluster_task_id_t>(100, "task_id", result.header.task_id);
	deserializer.ReadProperty<persistent_table_id_t>(101, "table_id", result.header.table_id);
	deserializer.ReadProperty<layout_version_t>(102, "prepared_layout_version", result.header.prepared_layout_version);
	deserializer.ReadProperty<sort_order_id_t>(103, "sort_order_id", result.header.sort_order_id);
	deserializer.ReadProperty<sort_run_id_t>(104, "run_id", result.header.run_id);
	deserializer.ReadProperty<row_t>(105, "range_start", result.header.input_range.start);
	deserializer.ReadProperty<row_t>(106, "range_end", result.header.input_range.end);
	deserializer.ReadProperty<delete_sequence_t>(107, "last_applied_delete_sequence",
	                                             result.header.last_applied_delete_sequence);
	deserializer.ReadProperty<uint64_t>(108, "manifest_revision", result.header.manifest_revision);
	deserializer.ReadProperty<vector<SortColumnDefinition>>(109, "sort_columns", result.sort_columns);
	deserializer.ReadProperty<vector<ManifestColumnDefinition>>(110, "physical_columns", result.physical_columns);
	deserializer.ReadProperty<vector<RowGroupPhysicalIdentity>>(111, "old_groups", result.old_groups);
	deserializer.ReadList(112, "replacement_groups", [&](Deserializer::List &list, idx_t) {
		list.ReadObject(
		    [&](Deserializer &object) { result.replacement_groups.push_back(RowGroup::Deserialize(object)); });
	});
	deserializer.ReadProperty<vector<block_id_t>>(113, "all_referenced_blocks", result.all_referenced_blocks);
	deserializer.End();
	if (stream.GetPosition() != payload_size_p) {
		throw SerializationException("Replacement manifest payload has trailing data");
	}
	return result;
}

static void ValidateIdentity(const RowGroupPhysicalIdentity &identity,
                             const vector<ManifestColumnDefinition> &physical_columns) {
	if (identity.format_version != 1 || identity.start < 0 || identity.count == 0 || !identity.sealed ||
	    !identity.sort_metadata.IsValid() || identity.columns.size() != physical_columns.size()) {
		throw SerializationException("Replacement manifest contains an invalid old row group identity");
	}
	for (idx_t column_index = 0; column_index < identity.columns.size(); column_index++) {
		auto &identity_column = identity.columns[column_index];
		auto &manifest_column = physical_columns[column_index];
		if (identity_column.column_id != manifest_column.column_id || identity_column.type != manifest_column.type ||
		    !identity_column.column_data_pointer.IsValid()) {
			throw SerializationException(
			    "Replacement manifest old row group schema does not match its physical columns");
		}
	}
	if (identity.immutable_data_checksum != ComputeRowGroupPhysicalIdentityChecksumV1(identity)) {
		throw SerializationException("Replacement manifest old row group checksum mismatch");
	}
}

static bool IsValidMetadataPointer(const MetaBlockPointer &pointer, const vector<block_id_t> &referenced_blocks) {
	if (!pointer.IsValid() || pointer.GetBlockIndex() >= MetadataManager::METADATA_BLOCK_COUNT) {
		return false;
	}
	auto block_id = pointer.GetBlockId();
	return block_id >= 0 && std::binary_search(referenced_blocks.begin(), referenced_blocks.end(), block_id);
}

static void ValidateAdditionalMetadataBlocks(const PerColumnMetadataBlocks &metadata_blocks, idx_t column_count,
                                             const vector<block_id_t> &referenced_blocks) {
	bool has_column = false;
	bool has_block = false;
	idx_t previous_column = 0;
	for (auto &entry : metadata_blocks.data) {
		if (entry.is_column_index) {
			if ((has_column && !has_block) || entry.index >= column_count ||
			    (has_column && entry.index <= previous_column)) {
				throw SerializationException("Replacement manifest contains invalid per-column metadata blocks");
			}
			has_column = true;
			has_block = false;
			previous_column = entry.index;
			continue;
		}
		if (!has_column || !IsValidMetadataPointer(MetaBlockPointer(entry.index, 0), referenced_blocks)) {
			throw SerializationException("Replacement manifest contains an invalid additional metadata block");
		}
		has_block = true;
	}
	if (has_column && !has_block) {
		throw SerializationException("Replacement manifest contains invalid per-column metadata blocks");
	}
}

void ReplacementManifest::Validate() const {
	if (header.format_version != REPLACEMENT_MANIFEST_FORMAT_VERSION || header.task_id == hugeint_t(0, 0) ||
	    header.table_id == hugeint_t(0, 0) || header.sort_order_id == INVALID_SORT_ORDER_ID ||
	    header.run_id == INVALID_SORT_RUN_ID || header.input_range.start < 0 ||
	    header.input_range.start >= header.input_range.end || header.manifest_revision == 0 || sort_columns.empty() ||
	    physical_columns.empty() || old_groups.empty()) {
		throw SerializationException("Invalid replacement manifest envelope");
	}

	for (auto &column : physical_columns) {
		if (column.column_id == 0 || !column.type.IsValid()) {
			throw SerializationException("Replacement manifest contains an invalid physical column");
		}
	}
	for (idx_t column_index = 0; column_index < physical_columns.size(); column_index++) {
		for (idx_t previous_index = 0; previous_index < column_index; previous_index++) {
			if (physical_columns[column_index].column_id == physical_columns[previous_index].column_id) {
				throw SerializationException("Replacement manifest contains duplicate physical column IDs");
			}
		}
	}
	for (idx_t sort_index = 0; sort_index < sort_columns.size(); sort_index++) {
		auto &sort_column = sort_columns[sort_index];
		if (sort_column.column_id == 0 || sort_column.order_type != OrderType::ASCENDING ||
		    sort_column.null_order != OrderByNullType::NULLS_LAST ||
		    std::find_if(physical_columns.begin(), physical_columns.end(), [&](const ManifestColumnDefinition &column) {
			    return column.column_id == sort_column.column_id;
		    }) == physical_columns.end()) {
			throw SerializationException("Replacement manifest contains an invalid sort column");
		}
		for (idx_t previous_index = 0; previous_index < sort_index; previous_index++) {
			if (sort_column.column_id == sort_columns[previous_index].column_id) {
				throw SerializationException("Replacement manifest contains duplicate sort column IDs");
			}
		}
	}

	block_id_t previous_block = INVALID_BLOCK;
	for (auto block_id : all_referenced_blocks) {
		if (block_id < 0 || (previous_block != INVALID_BLOCK && block_id <= previous_block)) {
			throw SerializationException("Replacement manifest referenced blocks must be sorted and unique");
		}
		previous_block = block_id;
	}

	auto old_end = header.input_range.start;
	for (auto &old_group : old_groups) {
		ValidateIdentity(old_group, physical_columns);
		if (old_group.start != old_end || old_group.start >= header.input_range.end ||
		    old_group.count > NumericCast<idx_t>(header.input_range.end - old_group.start)) {
			throw SerializationException("Replacement manifest old row groups are not contiguous or are out of order");
		}
		old_end = old_group.start + NumericCast<row_t>(old_group.count);
	}
	if (old_groups.front().start != header.input_range.start || old_end != header.input_range.end) {
		throw SerializationException("Replacement manifest old row groups do not cover its input range endpoints");
	}

	auto replacement_end = NumericCast<uint64_t>(header.input_range.start);
	auto range_end = NumericCast<uint64_t>(header.input_range.end);
	for (auto &replacement : replacement_groups) {
		if (replacement.row_start != replacement_end || replacement.tuple_count == 0 ||
		    replacement.data_pointers.size() != physical_columns.size() ||
		    replacement.sort_metadata != RowGroupSortMetadata {header.sort_order_id, header.run_id} ||
		    !replacement.has_per_column_metadata_blocks || replacement.tuple_count > range_end - replacement_end) {
			throw SerializationException("Replacement manifest contains an invalid replacement row group");
		}
		for (auto &pointer : replacement.data_pointers) {
			if (!IsValidMetadataPointer(pointer, all_referenced_blocks)) {
				throw SerializationException("Replacement manifest contains an invalid column metadata pointer");
			}
		}
		for (auto &pointer : replacement.deletes_pointers) {
			if (!IsValidMetadataPointer(pointer, all_referenced_blocks)) {
				throw SerializationException("Replacement manifest contains an invalid delete metadata pointer");
			}
		}
		ValidateAdditionalMetadataBlocks(replacement.per_column_metadata_blocks, physical_columns.size(),
		                                 all_referenced_blocks);
		replacement_end += replacement.tuple_count;
	}
	if (replacement_end > range_end) {
		throw SerializationException("Replacement manifest row IDs exceed its input range");
	}
	if (replacement_groups.empty() && !all_referenced_blocks.empty()) {
		throw SerializationException("Empty replacement manifest references unused blocks");
	}
}

void ReplacementManifest::Seal() {
	Validate();
	auto payload = SerializePayload();
	if (payload.size() > REPLACEMENT_MANIFEST_MAX_PAYLOAD_SIZE) {
		throw SerializationException("Replacement manifest payload exceeds the v1 size limit");
	}
	payload_size = payload.size();
	checksum = Checksum(payload.data(), payload.size());
}

void ReplacementManifest::VerifySeal() const {
	Validate();
	auto payload = SerializePayload();
	if (payload.size() != payload_size || payload.size() > REPLACEMENT_MANIFEST_MAX_PAYLOAD_SIZE ||
	    Checksum(payload.data(), payload.size()) != checksum) {
		throw SerializationException("Replacement manifest seal changed: expected size %llu, current size %llu",
		                             payload_size, payload.size());
	}
}

void ReplacementManifest::Write(WriteStream &stream) const {
	VerifySeal();
	auto payload = SerializePayload();
	stream.WriteData(const_data_ptr_cast(REPLACEMENT_MANIFEST_MAGIC), sizeof(REPLACEMENT_MANIFEST_MAGIC));
	stream.Write<uint32_t>(header.format_version);
	stream.Write<uint64_t>(payload_size);
	stream.Write<uint64_t>(checksum);
	stream.WriteData(payload.data(), payload.size());
}

ReplacementManifest ReplacementManifest::Read(ReadStream &stream) {
	char magic[sizeof(REPLACEMENT_MANIFEST_MAGIC)];
	stream.ReadData(data_ptr_cast(magic), sizeof(magic));
	if (memcmp(magic, REPLACEMENT_MANIFEST_MAGIC, sizeof(magic)) != 0) {
		throw SerializationException("Invalid replacement manifest magic");
	}
	auto format_version = stream.Read<uint32_t>();
	if (format_version != REPLACEMENT_MANIFEST_FORMAT_VERSION) {
		throw SerializationException("Unsupported replacement manifest format version %u", format_version);
	}
	auto payload_size = stream.Read<uint64_t>();
	auto checksum = stream.Read<uint64_t>();
	if (payload_size == 0 || payload_size > REPLACEMENT_MANIFEST_MAX_PAYLOAD_SIZE) {
		throw SerializationException("Invalid replacement manifest payload size %llu", payload_size);
	}
	vector<uint8_t> payload(NumericCast<idx_t>(payload_size));
	stream.ReadData(payload.data(), payload.size());
	if (Checksum(payload.data(), payload.size()) != checksum) {
		throw SerializationException("Replacement manifest checksum mismatch");
	}
	auto result = DeserializePayload(payload.data(), payload.size());
	result.header.format_version = format_version;
	result.payload_size = payload_size;
	result.checksum = checksum;
	result.Validate();
	return result;
}

} // namespace duckdb
