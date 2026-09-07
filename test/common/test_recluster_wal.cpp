#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/storage/wal_entry.hpp"

using namespace duckdb; // NOLINT

class ReclusterWALFieldIdSerializer : public Serializer {
public:
	const duckdb::vector<field_id_t> &GetFieldIds() const {
		return field_ids;
	}

protected:
	void OnPropertyBegin(field_id_t field_id, const char *) final {
		RecordField(field_id);
	}
	void OnPropertyEnd() final {
	}
	void OnOptionalPropertyBegin(field_id_t field_id, const char *, bool present) final {
		if (present) {
			RecordField(field_id);
		}
	}
	void OnOptionalPropertyEnd(bool) final {
	}
	void OnObjectBegin() final {
		object_depth++;
	}
	void OnObjectEnd() final {
		object_depth--;
	}
	void OnListBegin(idx_t) final {
	}
	void OnListEnd() final {
	}
	void OnNullableBegin(bool) final {
	}
	void OnNullableEnd() final {
	}
	void WriteNull() final {
	}
	void WriteValue(bool) final {
	}
	void WriteValue(uint8_t) final {
	}
	void WriteValue(int8_t) final {
	}
	void WriteValue(uint16_t) final {
	}
	void WriteValue(int16_t) final {
	}
	void WriteValue(uint32_t) final {
	}
	void WriteValue(int32_t) final {
	}
	void WriteValue(uint64_t) final {
	}
	void WriteValue(int64_t) final {
	}
	void WriteValue(hugeint_t) final {
	}
	void WriteValue(uhugeint_t) final {
	}
	void WriteValue(float) final {
	}
	void WriteValue(double) final {
	}
	void WriteValue(string_t) final {
	}
	void WriteValue(const string &) final {
	}
	void WriteValue(const char *) final {
	}
	void WriteDataPtr(const_data_ptr_t, idx_t) final {
	}

private:
	void RecordField(field_id_t field_id) {
		if (object_depth == 0) {
			field_ids.push_back(field_id);
		}
	}

private:
	idx_t object_depth = 0;
	duckdb::vector<field_id_t> field_ids;
};

template <class T>
static T RoundTripReclusterWALEntry(const T &entry) {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	serializer.Begin();
	entry.Serialize(serializer);
	serializer.End();

	MemoryStream source(stream.GetData(), stream.GetPosition());
	BinaryDeserializer deserializer(source);
	deserializer.Begin();
	auto result = T::Deserialize(deserializer);
	deserializer.End();
	REQUIRE(source.GetPosition() == stream.GetPosition());
	return result;
}

TEST_CASE("Recluster WAL records preserve the publication contract", "[storage][recluster_wal]") {
	REQUIRE(static_cast<uint8_t>(WALType::RECLUSTER) == 32);
	REQUIRE(static_cast<uint8_t>(WALType::RECLUSTER_DELETE) == 33);
	REQUIRE(EnumUtil::ToString(WALType::RECLUSTER) == "RECLUSTER");
	REQUIRE(EnumUtil::FromString<WALType>("RECLUSTER_DELETE") == WALType::RECLUSTER_DELETE);

	WALReclusterEntry header;
	header.table_id = hugeint_t(11, 12);
	header.task_id = hugeint_t(21, 22);
	header.expected_layout_version = 7;
	header.target_layout_version = 8;
	header.range_start = 2048;
	header.range_end = 8192;
	header.manifest_pointer = MetaBlockPointer(91, 17);
	header.manifest_size = 12345;
	header.manifest_checksum = 67890;
	header.journal_resolved_through = 44;
	header.final_delete_row_count = STANDARD_VECTOR_SIZE + 3;
	header.delete_chunk_count = 2;
	header.Validate();
	ReclusterWALFieldIdSerializer header_field_serializer;
	header.Serialize(header_field_serializer);
	duckdb::vector<field_id_t> expected_header_fields {101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112};
	REQUIRE(header_field_serializer.GetFieldIds() == expected_header_fields);

	auto loaded_header = RoundTripReclusterWALEntry(header);
	REQUIRE(loaded_header.table_id == header.table_id);
	REQUIRE(loaded_header.task_id == header.task_id);
	REQUIRE(loaded_header.expected_layout_version == 7);
	REQUIRE(loaded_header.target_layout_version == 8);
	REQUIRE(loaded_header.range_start == 2048);
	REQUIRE(loaded_header.range_end == 8192);
	REQUIRE(loaded_header.manifest_pointer == header.manifest_pointer);
	REQUIRE(loaded_header.manifest_size == 12345);
	REQUIRE(loaded_header.manifest_checksum == 67890);
	REQUIRE(loaded_header.journal_resolved_through == 44);
	REQUIRE(loaded_header.final_delete_row_count == STANDARD_VECTOR_SIZE + 3);
	REQUIRE(loaded_header.delete_chunk_count == 2);

	WALReclusterDeleteEntry delete_entry;
	delete_entry.table_id = header.table_id;
	delete_entry.task_id = header.task_id;
	delete_entry.chunk_index = 1;
	delete_entry.new_rowids = {2048, 4097, 8191};
	delete_entry.Validate();
	ReclusterWALFieldIdSerializer delete_field_serializer;
	delete_entry.Serialize(delete_field_serializer);
	duckdb::vector<field_id_t> expected_delete_fields {101, 102, 103, 104};
	REQUIRE(delete_field_serializer.GetFieldIds() == expected_delete_fields);
	auto loaded_delete = RoundTripReclusterWALEntry(delete_entry);
	REQUIRE(loaded_delete.table_id == delete_entry.table_id);
	REQUIRE(loaded_delete.task_id == delete_entry.task_id);
	REQUIRE(loaded_delete.chunk_index == 1);
	REQUIRE(loaded_delete.new_rowids == delete_entry.new_rowids);
}

TEST_CASE("Recluster WAL records reject invalid envelopes", "[storage][recluster_wal]") {
	WALReclusterEntry header;
	REQUIRE_THROWS(header.Validate());
	header.table_id = hugeint_t(1, 1);
	header.task_id = hugeint_t(2, 2);
	header.expected_layout_version = 3;
	header.target_layout_version = 4;
	header.range_start = 0;
	header.range_end = 2048;
	header.manifest_pointer = MetaBlockPointer(10, 8);
	header.manifest_size = 100;
	header.final_delete_row_count = 1;
	REQUIRE_THROWS(header.Validate());
	header.delete_chunk_count = 1;
	REQUIRE_NOTHROW(header.Validate());
	header.final_delete_row_count = 2;
	header.delete_chunk_count = 2;
	REQUIRE_NOTHROW(header.Validate());
	header.delete_chunk_count = 3;
	REQUIRE_THROWS(header.Validate());
	header.final_delete_row_count = STANDARD_VECTOR_SIZE + 1;
	header.delete_chunk_count = 1;
	REQUIRE_THROWS(header.Validate());
	header.delete_chunk_count = 2;
	REQUIRE_NOTHROW(header.Validate());
	header.target_layout_version = 5;
	REQUIRE_THROWS(header.Validate());

	WALReclusterDeleteEntry delete_entry;
	delete_entry.table_id = header.table_id;
	delete_entry.task_id = header.task_id;
	REQUIRE_THROWS(delete_entry.Validate());
	delete_entry.new_rowids = {-1};
	REQUIRE_THROWS(delete_entry.Validate());
	delete_entry.new_rowids = {0};
	REQUIRE_NOTHROW(delete_entry.Validate());
	delete_entry.new_rowids.resize(STANDARD_VECTOR_SIZE + 1);
	REQUIRE_THROWS(delete_entry.Validate());
}
