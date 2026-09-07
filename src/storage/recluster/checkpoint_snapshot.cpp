#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/storage_compatibility.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"

namespace duckdb {

void RowGroupColumnPhysicalIdentity::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<persistent_column_id_t>(100, "column_id", column_id);
	serializer.WriteProperty<LogicalType>(101, "type", type);
	serializer.WriteProperty<MetaBlockPointer>(102, "column_data_pointer", column_data_pointer);
	serializer.WriteProperty<vector<MetaBlockPointer>>(103, "additional_metadata_pointers",
	                                                   additional_metadata_pointers);
}

RowGroupColumnPhysicalIdentity RowGroupColumnPhysicalIdentity::Deserialize(Deserializer &deserializer) {
	RowGroupColumnPhysicalIdentity result;
	deserializer.ReadProperty<persistent_column_id_t>(100, "column_id", result.column_id);
	deserializer.ReadProperty<LogicalType>(101, "type", result.type);
	deserializer.ReadProperty<MetaBlockPointer>(102, "column_data_pointer", result.column_data_pointer);
	deserializer.ReadProperty<vector<MetaBlockPointer>>(103, "additional_metadata_pointers",
	                                                    result.additional_metadata_pointers);
	return result;
}

void RowGroupPhysicalIdentity::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<uint32_t>(100, "format_version", format_version);
	serializer.WriteProperty<row_t>(101, "start", start);
	serializer.WriteProperty<idx_t>(102, "count", count);
	serializer.WriteProperty<bool>(103, "sealed", sealed);
	serializer.WriteProperty<vector<RowGroupColumnPhysicalIdentity>>(104, "columns", columns);
	serializer.WriteProperty<sort_order_id_t>(105, "sort_order_id", sort_metadata.sort_order_id);
	serializer.WriteProperty<sort_run_id_t>(106, "run_id", sort_metadata.run_id);
	serializer.WriteProperty<uint64_t>(107, "immutable_data_checksum", immutable_data_checksum);
}

RowGroupPhysicalIdentity RowGroupPhysicalIdentity::Deserialize(Deserializer &deserializer) {
	RowGroupPhysicalIdentity result;
	deserializer.ReadProperty<uint32_t>(100, "format_version", result.format_version);
	deserializer.ReadProperty<row_t>(101, "start", result.start);
	deserializer.ReadProperty<idx_t>(102, "count", result.count);
	deserializer.ReadProperty<bool>(103, "sealed", result.sealed);
	deserializer.ReadProperty<vector<RowGroupColumnPhysicalIdentity>>(104, "columns", result.columns);
	deserializer.ReadProperty<sort_order_id_t>(105, "sort_order_id", result.sort_metadata.sort_order_id);
	deserializer.ReadProperty<sort_run_id_t>(106, "run_id", result.sort_metadata.run_id);
	deserializer.ReadProperty<uint64_t>(107, "immutable_data_checksum", result.immutable_data_checksum);
	return result;
}

bool RowGroupColumnPhysicalIdentity::operator==(const RowGroupColumnPhysicalIdentity &other) const {
	return column_id == other.column_id && type == other.type && column_data_pointer == other.column_data_pointer &&
	       additional_metadata_pointers == other.additional_metadata_pointers;
}

bool RowGroupPhysicalIdentity::operator==(const RowGroupPhysicalIdentity &other) const {
	return format_version == other.format_version && start == other.start && count == other.count &&
	       sealed == other.sealed && columns == other.columns && sort_metadata == other.sort_metadata &&
	       immutable_data_checksum == other.immutable_data_checksum;
}

bool CheckpointLayoutSnapshot::operator==(const CheckpointLayoutSnapshot &other) const {
	return checkpoint_number == other.checkpoint_number && storage_generation_id == other.storage_generation_id &&
	       row_groups == other.row_groups;
}

template <class T>
static void AppendLittleEndian(vector<uint8_t> &buffer, T value) {
	using UNSIGNED_TYPE = typename std::make_unsigned<T>::type;
	auto unsigned_value = static_cast<UNSIGNED_TYPE>(value);
	for (idx_t byte_index = 0; byte_index < sizeof(T); byte_index++) {
		buffer.push_back(static_cast<uint8_t>(unsigned_value & 0xff));
		unsigned_value >>= 8;
	}
}

static void AppendPointer(vector<uint8_t> &buffer, const MetaBlockPointer &pointer) {
	AppendLittleEndian<uint64_t>(buffer, pointer.block_pointer);
	AppendLittleEndian<uint64_t>(buffer, pointer.offset);
}

static void AppendLogicalTypeV1(vector<uint8_t> &buffer, const LogicalType &type) {
	MemoryStream stream;
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V2_0_0);
	BinarySerializer::Serialize(type, stream, options);
	auto size = NumericCast<uint32_t>(stream.GetPosition());
	AppendLittleEndian<uint32_t>(buffer, size);
	buffer.insert(buffer.end(), stream.GetData(), stream.GetData() + size);
}

uint64_t ComputeRowGroupPhysicalIdentityChecksumV1(const RowGroupPhysicalIdentity &identity) {
	if (identity.format_version != 1) {
		throw InternalException("Unsupported row group physical identity format version %u", identity.format_version);
	}

	vector<uint8_t> canonical;
	canonical.reserve(64 + identity.columns.size() * 64);
	canonical.insert(canonical.end(), {'R', 'G', 'I', '1'});
	AppendLittleEndian<uint32_t>(canonical, identity.format_version);
	AppendLittleEndian<int64_t>(canonical, identity.start);
	AppendLittleEndian<uint64_t>(canonical, identity.count);
	AppendLittleEndian<uint8_t>(canonical, identity.sealed ? 1 : 0);
	AppendLittleEndian<uint64_t>(canonical, identity.sort_metadata.sort_order_id);
	AppendLittleEndian<uint64_t>(canonical, identity.sort_metadata.run_id);
	AppendLittleEndian<uint32_t>(canonical, NumericCast<uint32_t>(identity.columns.size()));
	for (auto &column : identity.columns) {
		AppendLittleEndian<uint64_t>(canonical, column.column_id);
		AppendLogicalTypeV1(canonical, column.type);
		AppendPointer(canonical, column.column_data_pointer);
		AppendLittleEndian<uint32_t>(canonical, NumericCast<uint32_t>(column.additional_metadata_pointers.size()));
		for (auto &pointer : column.additional_metadata_pointers) {
			AppendPointer(canonical, pointer);
		}
	}
	return Checksum(canonical.data(), canonical.size());
}

optional<RowGroupPhysicalIdentity> ComputeRowGroupPhysicalIdentityV1(const RowGroup &row_group, row_t row_start,
                                                                     const vector<ColumnDefinition> &columns) {
	if (!row_group.IsPersistent() || !row_group.HasPerColumnMetadataBlocks()) {
		return nullopt;
	}
	if (row_start < 0) {
		throw InternalException("Cannot identify a row group with a negative row start");
	}
	auto &column_pointers = row_group.GetColumnStartPointers();
	if (column_pointers.size() != columns.size()) {
		throw InternalException("Row group physical column count does not match its table schema");
	}

	vector<idx_t> column_indexes;
	column_indexes.reserve(columns.size());
	for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
		column_indexes.push_back(column_index);
	}
	auto additional_blocks = row_group.GetPerColumnMetadataBlocks(column_indexes);
	if (additional_blocks.size() != columns.size()) {
		throw InternalException("Row group per-column metadata does not match its table schema");
	}

	RowGroupPhysicalIdentity result;
	result.start = row_start;
	result.count = row_group.count.load();
	// A checkpointed row group in a table with sort history will not accept more rows, even without a sort label.
	result.sealed = row_group.IsSealed() || row_group.GetTableInfo().HasSortStorage();
	result.sort_metadata = row_group.GetSortMetadata();
	result.columns.reserve(columns.size());
	for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
		auto &column = columns[column_index];
		if (column.PersistentColumnId() == 0) {
			throw InternalException("Cannot identify a row group without stable physical column IDs");
		}
		RowGroupColumnPhysicalIdentity column_identity;
		column_identity.column_id = column.PersistentColumnId();
		column_identity.type = column.Type();
		column_identity.column_data_pointer = column_pointers[column_index];
		column_identity.additional_metadata_pointers.reserve(additional_blocks[column_index].size());
		for (auto block_id : additional_blocks[column_index]) {
			column_identity.additional_metadata_pointers.emplace_back(block_id, 0);
		}
		result.columns.push_back(std::move(column_identity));
	}
	result.immutable_data_checksum = ComputeRowGroupPhysicalIdentityChecksumV1(result);
	return result;
}

bool MatchRowGroupPhysicalIdentityV1(const RowGroupCollectionSnapshot &snapshot,
                                     const vector<ColumnDefinition> &columns, const RowGroupPhysicalIdentity &expected,
                                     LayoutRowGroupEntry &result) {
	if (!snapshot.Lookup(expected.start, result) || result.row_start != expected.start ||
	    result.GetRowEnd() != expected.start + NumericCast<row_t>(expected.count)) {
		return false;
	}
	auto identity = ComputeRowGroupPhysicalIdentityV1(*result.row_group, result.row_start, columns);
	return identity && *identity == expected;
}

bool MatchRowGroupPhysicalIdentitiesV1(const RowGroupCollectionSnapshot &snapshot,
                                       const vector<ColumnDefinition> &columns,
                                       const vector<RowGroupPhysicalIdentity> &expected) {
	for (auto &row_group : expected) {
		LayoutRowGroupEntry current;
		if (!MatchRowGroupPhysicalIdentityV1(snapshot, columns, row_group, current)) {
			return false;
		}
	}
	return true;
}

optional_idx FindCheckpointRowGroups(const CheckpointLayoutSnapshot &checkpoint,
                                     const vector<RowGroupPhysicalIdentity> &expected) {
	if (expected.empty()) {
		return optional_idx();
	}
	auto entry =
	    std::lower_bound(checkpoint.row_groups.begin(), checkpoint.row_groups.end(), expected.front().start,
	                     [](const RowGroupPhysicalIdentity &identity, row_t start) { return identity.start < start; });
	if (entry == checkpoint.row_groups.end() ||
	    expected.size() > NumericCast<idx_t>(checkpoint.row_groups.end() - entry) ||
	    !std::equal(expected.begin(), expected.end(), entry)) {
		return optional_idx();
	}
	return optional_idx(NumericCast<idx_t>(entry - checkpoint.row_groups.begin()));
}

optional<CheckpointLayoutSnapshot> BuildCheckpointLayoutSnapshot(RowGroupCollection &collection,
                                                                 const vector<ColumnDefinition> &columns,
                                                                 uint64_t checkpoint_number) {
	CheckpointLayoutSnapshot result;
	result.checkpoint_number = checkpoint_number;
	result.storage_generation_id = collection.GetStorageGenerationId();
	LayoutRowGroupCursor cursor(collection.GetCurrentSnapshot());
	LayoutRowGroupEntry entry;
	while (cursor.Next(entry)) {
		auto identity = ComputeRowGroupPhysicalIdentityV1(*entry.row_group, entry.row_start, columns);
		if (!identity) {
			return nullopt;
		}
		result.row_groups.push_back(std::move(*identity));
	}
	return result;
}

} // namespace duckdb
