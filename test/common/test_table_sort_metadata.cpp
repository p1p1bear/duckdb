#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

using namespace duckdb;

template <class T>
struct SortMetadataSerializationEnvelope {
	explicit SortMetadataSerializationEnvelope(T value_p) : value(std::move(value_p)) {
	}

	T value;

	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty<T>(100, "value", value);
	}

	static duckdb::unique_ptr<SortMetadataSerializationEnvelope<T>> Deserialize(Deserializer &deserializer) {
		auto value = deserializer.ReadProperty<T>(100, "value");
		return make_uniq<SortMetadataSerializationEnvelope<T>>(std::move(value));
	}
};

template <class T>
static T CopySortMetadataValue(const T &input) {
	return input;
}

static ColumnDefinition CopySortMetadataValue(const ColumnDefinition &input) {
	return input.Copy();
}

template <class T>
static T SortMetadataRoundTrip(const T &input, StorageCompatibility compatibility = StorageCompatibility::Latest()) {
	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.storage_compatibility = compatibility;
	SortMetadataSerializationEnvelope<T> envelope {CopySortMetadataValue(input)};
	BinarySerializer::Serialize(envelope, stream, options);
	stream.Rewind();
	auto output = BinaryDeserializer::Deserialize<SortMetadataSerializationEnvelope<T>>(stream);
	return std::move(output->value);
}

TEST_CASE("Table sort catalog metadata round trips", "[storage][sort_metadata]") {
	TableSortCatalogPostImage input;
	input.table_metadata.table_id = hugeint_t(42, 84);
	input.table_metadata.next_column_id = 4;
	input.table_metadata.current_sort_order_id = 2;
	input.table_metadata.next_sort_order_id = 3;
	input.table_metadata.definitions = {{1, {{1, OrderType::ASCENDING, OrderByNullType::NULLS_LAST}}},
	                                    {2,
	                                     {{2, OrderType::ASCENDING, OrderByNullType::NULLS_LAST},
	                                      {3, OrderType::ASCENDING, OrderByNullType::NULLS_LAST}}}};
	input.columns = {{0, "id", LogicalType::BIGINT, 1}, {1, "payload", LogicalType::VARCHAR, 2}};

	auto output = SortMetadataRoundTrip(input);
	REQUIRE(output == input);
	REQUIRE(output.table_metadata.IsEnabled());
	REQUIRE(output.table_metadata.GetCurrent() != nullptr);
	REQUIRE(output.table_metadata.GetCurrent()->sort_order_id == 2);
	REQUIRE(output.table_metadata.GetDefinition(1) != nullptr);
	REQUIRE(output.table_metadata.GetDefinition(99) == nullptr);
}

TEST_CASE("Column definitions preserve persistent column IDs", "[storage][sort_metadata]") {
	ColumnDefinition input("payload", LogicalType::VARCHAR);
	input.SetPersistentColumnId(42);

	auto copy = input.Copy();
	REQUIRE(copy.PersistentColumnId() == 42);

	auto output = SortMetadataRoundTrip(input);
	REQUIRE(output.PersistentColumnId() == 42);
	REQUIRE(output.Name() == input.Name());
	REQUIRE(output.Type() == input.Type());

	auto legacy_output = SortMetadataRoundTrip(input, StorageCompatibility::FromString("v1.5.5"));
	REQUIRE(legacy_output.PersistentColumnId() == 0);
}

TEST_CASE("Row group sort metadata requires paired identifiers", "[storage][sort_metadata]") {
	REQUIRE(RowGroupSortMetadata().IsValid());
	REQUIRE(!RowGroupSortMetadata().IsSorted());
	REQUIRE(RowGroupSortMetadata {1, 2}.IsValid());
	REQUIRE(RowGroupSortMetadata {1, 2}.IsSorted());
	REQUIRE(!RowGroupSortMetadata {1, 0}.IsValid());
	REQUIRE(!RowGroupSortMetadata {0, 2}.IsValid());
}
