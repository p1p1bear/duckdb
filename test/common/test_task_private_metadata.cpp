#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

class TestTaskPrivateMetadataWriter : public MetadataWriter {
public:
	explicit TestTaskPrivateMetadataWriter(TaskPrivateMetadataBlockOwner &owner_p)
	    : MetadataWriter(owner_p.GetManager()), owner(owner_p) {
	}

protected:
	MetadataHandle NextHandle() override {
		return owner.AllocateHandle();
	}

private:
	TaskPrivateMetadataBlockOwner &owner;
};

static BlockManager &GetTableBlockManager(Connection &con) {
	optional_ptr<BlockManager> result;
	con.context->RunFunctionInTransaction([&]() {
		auto &entry = Catalog::GetEntry<DuckTableEntry>(*con.context, QualifiedName(Identifier("tbl")));
		result = entry.GetStorage().GetTableIOManager().GetBlockManagerForRowData();
	});
	return *result;
}

static void CheckMetadataPayload(MetadataManager &manager, MetaBlockPointer pointer,
                                 const duckdb::vector<uint8_t> &expected) {
	duckdb::vector<uint8_t> actual(expected.size());
	MetadataReader reader(manager, pointer);
	reader.ReadData(actual.data(), actual.size());
	REQUIRE(actual == expected);
}

TEST_CASE("Task-private metadata blocks remain invisible to checkpoints", "[storage][task_private_metadata]") {
	auto path = TestCreatePath("task_private_metadata.db");
	DeleteDatabase(path);
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS private_db (STORAGE_VERSION 'v2.0.0')"));
	REQUIRE_NO_FAIL(con.Query("USE private_db"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT private_db"));

	auto &block_manager = GetTableBlockManager(con);
	auto &metadata_manager = block_manager.GetMetadataManager();
	auto initial_metadata_count = metadata_manager.BlockCount();
	auto first_private_block = block_manager.PeekFreeBlockId();
	auto owner = metadata_manager.CreateTaskPrivateBlockOwner();

	duckdb::vector<uint8_t> payload(metadata_manager.GetMetadataBlockSize() * 2 + 17);
	for (idx_t index = 0; index < payload.size(); index++) {
		payload[index] = NumericCast<uint8_t>((index * 37) % 251);
	}

	MetaBlockPointer root_pointer;
	{
		TestTaskPrivateMetadataWriter writer(*owner);
		root_pointer = writer.GetMetaBlockPointer();
		writer.WriteData(payload.data(), payload.size());
		writer.Flush();
	}
	REQUIRE(root_pointer.GetBlockId() == first_private_block);
	REQUIRE(owner->GetBlockIds() == duckdb::vector<block_id_t> {first_private_block});
	REQUIRE(metadata_manager.BlockCount() == initial_metadata_count);

	metadata_manager.Flush();
	REQUIRE_THROWS_AS(owner->MarkPublished(), InternalException);
	owner->Flush();
	CheckMetadataPayload(metadata_manager, root_pointer, payload);

	for (auto &info : metadata_manager.GetMetadataInfo()) {
		REQUIRE(info.block_id != first_private_block);
	}
	for (auto &block : metadata_manager.GetBlocks()) {
		REQUIRE(block->BlockId() != first_private_block);
	}

	REQUIRE_NO_FAIL(con.Query("CHECKPOINT private_db"));
	REQUIRE(metadata_manager.BlockCount() >= initial_metadata_count);
	for (auto &info : metadata_manager.GetMetadataInfo()) {
		REQUIRE(info.block_id != first_private_block);
	}
	CheckMetadataPayload(metadata_manager, root_pointer, payload);

	owner->Abort();
	owner.reset();
	REQUIRE(block_manager.PeekFreeBlockId() == first_private_block);
	DeleteDatabase(path);
}
