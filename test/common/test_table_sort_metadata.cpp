#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
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

TEST_CASE("Create table sort metadata copies, serializes, and reconstructs SQL", "[storage][sort_metadata]") {
	CreateTableInfo input(QualifiedName(Identifier("events")));
	ColumnDefinition event_time("event_time", LogicalType::TIMESTAMP);
	event_time.SetPersistentColumnId(1);
	input.columns.AddColumn(std::move(event_time));
	ColumnDefinition payload("payload", LogicalType::VARCHAR);
	payload.SetPersistentColumnId(2);
	input.columns.AddColumn(std::move(payload));
	input.sort_metadata = TableSortCatalogMetadata();
	input.sort_metadata->table_id = hugeint_t(42, 84);
	input.sort_metadata->next_column_id = 3;
	input.sort_metadata->current_sort_order_id = 1;
	input.sort_metadata->next_sort_order_id = 2;
	input.sort_metadata->definitions = {{1, {{1, OrderType::ASCENDING, OrderByNullType::NULLS_LAST}}}};

	auto copied_info = input.Copy();
	auto &copied = copied_info->Cast<CreateTableInfo>();
	REQUIRE(copied.sort_metadata == input.sort_metadata);
	REQUIRE(copied.sort_orders.empty());
	REQUIRE(copied.ToString() == "CREATE TABLE events(event_time TIMESTAMP, payload VARCHAR) SORTED BY "
	                             "(event_time ASC NULLS LAST);");

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::Latest();
	BinarySerializer::Serialize(input, stream, options);
	stream.Rewind();
	auto output_info = BinaryDeserializer::Deserialize<CreateInfo>(stream);
	auto &output = output_info->Cast<CreateTableInfo>();
	REQUIRE(output.sort_metadata == input.sort_metadata);
	REQUIRE(output.sort_orders.empty());
	REQUIRE(output.columns.GetColumn(LogicalIndex(0)).PersistentColumnId() == 1);
	REQUIRE(output.ToString() == copied.ToString());

	options.storage_compatibility = StorageCompatibility::FromString("v1.5.5");
	REQUIRE_THROWS_AS(BinarySerializer::Serialize(input, stream, options), SerializationException);
}

TEST_CASE("Create table parser preserves sort order modifiers", "[parser][sort_metadata]") {
	Parser parser;
	parser.ParseQuery("CREATE TABLE events(tenant_id BIGINT, event_time TIMESTAMP) "
	                  "SORTED BY (tenant_id, event_time ASC NULLS LAST)");
	REQUIRE(parser.statements.size() == 1);
	auto &statement = parser.statements[0]->Cast<CreateStatement>();
	auto &info = statement.info->Cast<CreateTableInfo>();
	REQUIRE(info.sort_keys.size() == 2);
	REQUIRE(info.sort_orders.size() == 2);
	REQUIRE(info.sort_orders[0].type == OrderType::ORDER_DEFAULT);
	REQUIRE(info.sort_orders[0].null_order == OrderByNullType::ORDER_DEFAULT);
	REQUIRE(info.sort_orders[1].type == OrderType::ASCENDING);
	REQUIRE(info.sort_orders[1].null_order == OrderByNullType::NULLS_LAST);
	for (idx_t i = 0; i < info.sort_orders.size(); i++) {
		REQUIRE(ParsedExpression::Equals(info.sort_keys[i], info.sort_orders[i].expression));
	}

	Parser modified_parser;
	modified_parser.ParseQuery("CREATE TABLE rejected(i INTEGER) SORTED BY (i DESC NULLS FIRST)");
	auto &modified_info = modified_parser.statements[0]->Cast<CreateStatement>().info->Cast<CreateTableInfo>();
	REQUIRE(modified_info.sort_orders[0].type == OrderType::DESCENDING);
	REQUIRE(modified_info.sort_orders[0].null_order == OrderByNullType::NULLS_FIRST);
}

TEST_CASE("Create table sort key projections normalize and validate", "[storage][sort_metadata]") {
	CreateTableInfo input(QualifiedName(Identifier("events")));
	input.columns.AddColumn(ColumnDefinition("event_time", LogicalType::TIMESTAMP));
	input.sort_keys.push_back(make_uniq<ColumnRefExpression>("event_time"));

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromString("v1.5.5");
	BinarySerializer::Serialize(input, stream, options);
	stream.Rewind();
	auto legacy_output_info = BinaryDeserializer::Deserialize<CreateInfo>(stream);
	auto &legacy_output = legacy_output_info->Cast<CreateTableInfo>();
	REQUIRE(!legacy_output.sort_metadata);
	REQUIRE(legacy_output.sort_keys.size() == 1);
	REQUIRE(legacy_output.sort_orders.size() == 1);
	REQUIRE(legacy_output.sort_orders[0].type == OrderType::ORDER_DEFAULT);
	REQUIRE(legacy_output.sort_orders[0].null_order == OrderByNullType::ORDER_DEFAULT);

	CreateTableInfo mismatched(QualifiedName(Identifier("events")));
	mismatched.sort_keys.push_back(make_uniq<ColumnRefExpression>("event_time"));
	mismatched.sort_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                                    make_uniq<ColumnRefExpression>("other_column"));
	REQUIRE_THROWS_AS(mismatched.NormalizeLegacySortKeys(), SerializationException);

	CreateTableInfo mixed(QualifiedName(Identifier("events")));
	mixed.sort_metadata = TableSortCatalogMetadata();
	mixed.sort_keys.push_back(make_uniq<ColumnRefExpression>("event_time"));
	REQUIRE_THROWS_AS(mixed.NormalizeLegacySortKeys(), SerializationException);
}

static TableSortCatalogPostImage MakeSortPostImage() {
	TableSortCatalogPostImage result;
	result.table_metadata.table_id = hugeint_t(42, 84);
	result.table_metadata.next_column_id = 2;
	result.table_metadata.current_sort_order_id = 1;
	result.table_metadata.next_sort_order_id = 2;
	result.table_metadata.definitions = {{1, {{1, OrderType::ASCENDING, OrderByNullType::NULLS_LAST}}}};
	result.columns = {{0, "event_time", LogicalType::TIMESTAMP, 1}};
	return result;
}

TEST_CASE("Table alter post-images survive copies and serialization", "[storage][sort_metadata]") {
	AlterEntryData data(QualifiedName(Identifier("events")), OnEntryNotFound::THROW_EXCEPTION);
	duckdb::vector<OrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<ColumnRefExpression>("event_time"));
	SetSortedByInfo input(data, std::move(orders));
	input.sort_post_image = MakeSortPostImage();

	auto copied = input.Copy();
	REQUIRE(copied->Cast<AlterTableInfo>().sort_post_image == input.sort_post_image);

	RenameColumnInfo rename(data, "event_time", "created_at");
	rename.sort_post_image = input.sort_post_image;
	REQUIRE(rename.Copy()->Cast<AlterTableInfo>().sort_post_image == input.sort_post_image);

	AddColumnInfo add(data, ColumnDefinition("payload", LogicalType::VARCHAR), false);
	add.sort_post_image = input.sort_post_image;
	REQUIRE(add.Copy()->Cast<AlterTableInfo>().sort_post_image == input.sort_post_image);

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::Latest();
	BinarySerializer::Serialize(input, stream, options);
	stream.Rewind();
	auto output_info = BinaryDeserializer::Deserialize<ParseInfo>(stream);
	auto &output = output_info->Cast<AlterInfo>().Cast<AlterTableInfo>();
	REQUIRE(output.sort_post_image == input.sort_post_image);
}

TEST_CASE("Row group sort metadata requires paired identifiers", "[storage][sort_metadata]") {
	REQUIRE(RowGroupSortMetadata().IsValid());
	REQUIRE(!RowGroupSortMetadata().IsSorted());
	REQUIRE(RowGroupSortMetadata {1, 2}.IsValid());
	REQUIRE(RowGroupSortMetadata {1, 2}.IsSorted());
	REQUIRE(!RowGroupSortMetadata {1, 0}.IsValid());
	REQUIRE(!RowGroupSortMetadata {0, 2}.IsValid());
}
