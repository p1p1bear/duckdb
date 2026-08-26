#include <duckdb/main/settings.hpp>

#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "test_helpers.hpp"
#include "zstd.h"

#include <array>
#include <cstring>

using namespace duckdb;

namespace duckdb {

class TemporaryFileCompressionTest {
public:
	struct Result {
		Result(TemporaryBufferSize size_p, TemporaryCompressionLevel level_p, AllocatedData data_p)
		    : size(size_p), level(level_p), data(std::move(data_p)) {
		}

		TemporaryBufferSize size;
		TemporaryCompressionLevel level;
		AllocatedData data;
	};

	static Result Compress(TemporaryFileManager &manager, TemporaryCompressionLevel level, FileBuffer &buffer) {
		AllocatedData compressed_buffer;
		auto result = manager.CompressBuffer(level, buffer, compressed_buffer);
		return {result.size, result.level, std::move(compressed_buffer)};
	}

	static void UpdateAdaptivity(TemporaryFileCompressionAdaptivity &adaptivity, TemporaryCompressionLevel level,
	                             TemporaryBufferSize size, int64_t duration) {
		adaptivity.UpdateInternal(level, size, duration);
	}

	static int64_t GetDuration(const TemporaryFileCompressionAdaptivity &adaptivity, TemporaryCompressionLevel level) {
		if (level == TemporaryCompressionLevel::UNCOMPRESSED) {
			return adaptivity.last_uncompressed_write_ns;
		}
		return adaptivity.last_compressed_writes_ns[adaptivity.LevelToIndex(level)];
	}

	static idx_t GetStoredSize(const TemporaryFileCompressionAdaptivity &adaptivity, TemporaryCompressionLevel level) {
		return adaptivity.last_compressed_sizes[adaptivity.LevelToIndex(level)];
	}

	static double GetCompressionAcceptance(const TemporaryFileCompressionAdaptivity &adaptivity,
	                                       TemporaryCompressionLevel level) {
		return adaptivity.last_compression_acceptance[adaptivity.LevelToIndex(level)];
	}

	static bool CompressionLevelIsBeneficial(const TemporaryFileCompressionAdaptivity &adaptivity,
	                                         TemporaryCompressionLevel level) {
		return adaptivity.CompressionLevelIsBeneficial(adaptivity.LevelToIndex(level));
	}
};

} // namespace duckdb

static unique_ptr<FileBuffer> CreateTemporaryCompressionBuffer(DatabaseInstance &db) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	return buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), Storage::DEFAULT_BLOCK_HEADER_SIZE,
	                                             nullptr);
}

static void FillPseudoRandom(FileBuffer &buffer, uint64_t state) {
	auto data = buffer.InternalBuffer();
	for (idx_t offset = 0; offset < buffer.AllocSize(); offset++) {
		state ^= state >> 12;
		state ^= state << 25;
		state ^= state >> 27;
		data[offset] = static_cast<uint8_t>(state * 2685821657736338717ULL);
	}
}

static bool TemporaryCompressionRoundTrips(FileBuffer &input, const TemporaryFileCompressionTest::Result &result) {
	if (!result.data.get() || result.data.GetSize() < sizeof(idx_t)) {
		return false;
	}
	const auto compressed_size = Load<idx_t>(result.data.get());
	if (compressed_size > result.data.GetSize() - sizeof(idx_t) ||
	    compressed_size + sizeof(idx_t) > static_cast<idx_t>(result.size)) {
		return false;
	}
	vector<uint8_t> decompressed(input.AllocSize());
	const auto decompressed_size = duckdb_zstd::ZSTD_decompress(decompressed.data(), decompressed.size(),
	                                                            result.data.get() + sizeof(idx_t), compressed_size);
	return !duckdb_zstd::ZSTD_isError(decompressed_size) && decompressed_size == input.AllocSize() &&
	       memcmp(decompressed.data(), input.InternalBuffer(), input.AllocSize()) == 0;
}

TEST_CASE("Temporary file compression caps output and round-trips", "[storage]") {
	DuckDB db(nullptr);
	atomic<idx_t> size_on_disk(0);
	TemporaryFileManager manager(*db.instance, string(), size_on_disk);
	auto buffer = CreateTemporaryCompressionBuffer(*db.instance);

	auto uncompressed =
	    TemporaryFileCompressionTest::Compress(manager, TemporaryCompressionLevel::UNCOMPRESSED, *buffer);
	CHECK(uncompressed.size == TemporaryBufferSize::DEFAULT);
	CHECK(uncompressed.level == TemporaryCompressionLevel::UNCOMPRESSED);
	CHECK_FALSE(uncompressed.data.IsSet());

	FillPseudoRandom(*buffer, 42);
	auto rejected =
	    TemporaryFileCompressionTest::Compress(manager, TemporaryCompressionLevel::ZSTD_MINUS_FIVE, *buffer);
	CHECK(rejected.size == TemporaryBufferSize::DEFAULT);
	CHECK(rejected.level == TemporaryCompressionLevel::ZSTD_MINUS_FIVE);
	CHECK_FALSE(rejected.data.IsSet());

	memset(buffer->InternalBuffer(), 0, buffer->AllocSize());
	for (auto level : {TemporaryCompressionLevel::ZSTD_MINUS_FIVE, TemporaryCompressionLevel::ZSTD_FIVE,
	                   TemporaryCompressionLevel::ZSTD_MINUS_FIVE}) {
		auto compressed = TemporaryFileCompressionTest::Compress(manager, level, *buffer);
		CHECK(compressed.size != TemporaryBufferSize::DEFAULT);
		CHECK(compressed.level == level);
		CHECK(compressed.data.GetSize() == static_cast<idx_t>(TemporaryBufferSize::S224K));
		CHECK(TemporaryCompressionRoundTrips(*buffer, compressed));
	}
}

TEST_CASE("Temporary file compression adaptivity warms up and tracks disk benefit", "[storage]") {
	TemporaryFileCompressionAdaptivity adaptivity;
	const array<TemporaryCompressionLevel, 7> levels {
	    TemporaryCompressionLevel::UNCOMPRESSED,     TemporaryCompressionLevel::ZSTD_MINUS_FIVE,
	    TemporaryCompressionLevel::ZSTD_MINUS_THREE, TemporaryCompressionLevel::ZSTD_MINUS_ONE,
	    TemporaryCompressionLevel::ZSTD_ONE,         TemporaryCompressionLevel::ZSTD_THREE,
	    TemporaryCompressionLevel::ZSTD_FIVE};
	const array<TemporaryBufferSize, 7> sizes {TemporaryBufferSize::DEFAULT, TemporaryBufferSize::DEFAULT,
	                                           TemporaryBufferSize::S224K,   TemporaryBufferSize::S128K,
	                                           TemporaryBufferSize::S64K,    TemporaryBufferSize::S32K,
	                                           TemporaryBufferSize::S32K};
	const array<int64_t, 7> durations {100, 200, 180, 160, 140, 120, 110};

	for (idx_t i = 0; i < levels.size(); i++) {
		CHECK(adaptivity.GetCompressionLevel() == levels[i]);
		TemporaryFileCompressionTest::UpdateAdaptivity(adaptivity, levels[i], sizes[i], durations[i]);
		CHECK(TemporaryFileCompressionTest::GetDuration(adaptivity, levels[i]) == durations[i]);
		if (levels[i] != TemporaryCompressionLevel::UNCOMPRESSED) {
			CHECK(TemporaryFileCompressionTest::GetStoredSize(adaptivity, levels[i]) == static_cast<idx_t>(sizes[i]));
		}
	}

	CHECK(TemporaryFileCompressionTest::GetCompressionAcceptance(
	          adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_FIVE) == Approx(0.0));
	CHECK(TemporaryFileCompressionTest::GetCompressionAcceptance(
	          adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_THREE) == Approx(1.0));
	CHECK_FALSE(TemporaryFileCompressionTest::CompressionLevelIsBeneficial(adaptivity,
	                                                                       TemporaryCompressionLevel::ZSTD_MINUS_FIVE));
	CHECK_FALSE(TemporaryFileCompressionTest::CompressionLevelIsBeneficial(
	    adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_THREE));
	CHECK(TemporaryFileCompressionTest::CompressionLevelIsBeneficial(adaptivity,
	                                                                 TemporaryCompressionLevel::ZSTD_MINUS_ONE));

	TemporaryFileCompressionAdaptivity noisy_rejection;
	TemporaryFileCompressionTest::UpdateAdaptivity(noisy_rejection, TemporaryCompressionLevel::UNCOMPRESSED,
	                                               TemporaryBufferSize::DEFAULT, 100);
	TemporaryFileCompressionTest::UpdateAdaptivity(noisy_rejection, TemporaryCompressionLevel::ZSTD_MINUS_FIVE,
	                                               TemporaryBufferSize::DEFAULT, 50);
	CHECK_FALSE(TemporaryFileCompressionTest::CompressionLevelIsBeneficial(noisy_rejection,
	                                                                       TemporaryCompressionLevel::ZSTD_MINUS_FIVE));

	TemporaryFileCompressionTest::UpdateAdaptivity(adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_FIVE,
	                                               TemporaryBufferSize::S128K, 360);
	CHECK(TemporaryFileCompressionTest::GetDuration(adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_FIVE) == 210);
	CHECK(TemporaryFileCompressionTest::GetStoredSize(adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_FIVE) ==
	      253952);
	CHECK(TemporaryFileCompressionTest::GetCompressionAcceptance(
	          adaptivity, TemporaryCompressionLevel::ZSTD_MINUS_FIVE) == Approx(1.0 / 16.0));
}

TEST_CASE("Capped temporary compression matches full-bound compression", "[storage]") {
	DuckDB db(nullptr);
	atomic<idx_t> size_on_disk(0);
	TemporaryFileManager manager(*db.instance, string(), size_on_disk);
	auto buffer = CreateTemporaryCompressionBuffer(*db.instance);
	vector<uint8_t> full_output(duckdb_zstd::ZSTD_compressBound(buffer->AllocSize()));
	const auto maximum_compressed_size = static_cast<idx_t>(TemporaryBufferSize::S224K);

	for (idx_t compressible_parts = 0; compressible_parts <= 16; compressible_parts++) {
		FillPseudoRandom(*buffer, 42 + compressible_parts);
		memset(buffer->InternalBuffer(), 0, buffer->AllocSize() / 16 * compressible_parts);
		for (auto level : {TemporaryCompressionLevel::ZSTD_MINUS_FIVE, TemporaryCompressionLevel::ZSTD_MINUS_THREE,
		                   TemporaryCompressionLevel::ZSTD_MINUS_ONE, TemporaryCompressionLevel::ZSTD_ONE,
		                   TemporaryCompressionLevel::ZSTD_THREE, TemporaryCompressionLevel::ZSTD_FIVE}) {
			const auto zstd_size =
			    duckdb_zstd::ZSTD_compress(full_output.data(), full_output.size(), buffer->InternalBuffer(),
			                               buffer->AllocSize(), static_cast<int>(level));
			REQUIRE_FALSE(duckdb_zstd::ZSTD_isError(zstd_size));
			const auto expected_compressed = sizeof(idx_t) + zstd_size <= maximum_compressed_size;
			auto result = TemporaryFileCompressionTest::Compress(manager, level, *buffer);
			CHECK(result.level == level);
			CHECK((result.size != TemporaryBufferSize::DEFAULT) == expected_compressed);
			if (expected_compressed) {
				REQUIRE(result.data.get());
				CHECK(Load<idx_t>(result.data.get()) == zstd_size);
				CHECK(memcmp(result.data.get() + sizeof(idx_t), full_output.data(), zstd_size) == 0);
			} else {
				CHECK_FALSE(result.data.get());
			}
		}
	}
}

TEST_CASE("Test storing a big string that exceeds buffer manager size", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));
	config->options.maximum_threads = 1;
	// ZSTD can store this in a smaller way, force uncompressed so the 5mb max test correctly fails
	config->SetOptionByName("force_compression", "uncompressed");

	uint64_t string_length = 64;
	uint64_t desired_size = 10000000; // desired size is 10MB
	uint64_t iteration = 2;
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert the big string
		DuckDB db(storage_database, config.get());
		Connection con(db);
		string big_string = string(string_length, 'a');
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, j BIGINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('" + big_string + "', 1)"));
		while (string_length < desired_size) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT repeat(a, 10), " + to_string(iteration) + " FROM test"));
			REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE j=" + to_string(iteration - 1)));
			iteration++;
			string_length *= 10;
		}

		// check the length
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	// now reload the database, but this time with a max memory of 5MB
	{
		config->options.maximum_memory = 5000000;
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// we can still select the integer
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
		// however the string is too big to fit in our buffer manager
		REQUIRE_FAIL(con.Query("SELECT LENGTH(a) FROM test"));
	}
	{
		// reloading with a bigger limit again makes it work
		config->options.maximum_memory = (idx_t)-1;
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT LENGTH(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(string_length)}));
		result = con.Query("SELECT j FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(iteration - 1)}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Modifying the buffer manager limit at runtime for an in-memory database", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=1"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_compression='uncompressed'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA temp_directory=''"));

	// initialize an in-memory database of size 10MB
	uint64_t table_size = (1000 * 1000) / sizeof(int);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3), (NULL)"));

	idx_t not_null_size = 3;
	idx_t size = 4;
	idx_t sum = 6;
	for (; size < table_size; size *= 2) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
		not_null_size *= 2;
		sum *= 2;
	}

	result = con.Query("SELECT COUNT(*), COUNT(a), SUM(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(not_null_size)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(sum)}));

	// we can set the memory limit to 1GB
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1GB'"));
	// but we cannot set it below 10MB
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='1MB'"));

	// if we make room by dropping the table, we can set it to 1MB though
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1MB'"));

	// also test that large strings are properly deleted
	// reset the memory limit
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit=-1"));

	// create a table with a large string (10MB)
	uint64_t string_length = 64;
	uint64_t desired_size = 10000000; // desired size is 10MB
	uint64_t iteration = 2;

	string big_string = string(string_length, 'a');
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, j BIGINT);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('" + big_string + "', 1)"));
	while (string_length < desired_size) {
		REQUIRE_NO_FAIL(
		    con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a, " + to_string(iteration) + " FROM test"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE j=" + to_string(iteration - 1)));
		iteration++;
		string_length *= 10;
	}

	// now we cannot set the memory limit to 1MB again
	REQUIRE_FAIL(con.Query("PRAGMA memory_limit='1MB'"));
	// but dropping the table allows us to set the memory limit to 1MB again
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA memory_limit='1MB'"));
}

TEST_CASE("Test buffer manager variable size allocations", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	idx_t requested_size = 424242;
	auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, requested_size, false);
	auto block = pin.GetBlockHandle();
	CHECK(buffer_manager.GetUsedMemory() >= requested_size + block->GetBlockHeaderSize());

	pin.Destroy();
	block.reset();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test buffer manager buffer re-use", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// Set memory limit to hold exactly 10 blocks
	idx_t pin_count = 10;
	auto block_alloc_size = Settings::Get<DefaultBlockSizeSetting>(*config);
	auto block_size = block_alloc_size - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", block_alloc_size * pin_count)));

	// Create 40 blocks, but don't hold the pin
	// They will be added to the eviction queue and the buffers will be re-used
	idx_t block_count = 40;
	duckdb::vector<duckdb::shared_ptr<BlockHandle>> blocks;
	blocks.reserve(block_count);
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		// used memory should increment by exactly one block at a time, up to 10
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * block_alloc_size);
	}

	// now pin them one by one - cycling through should trigger more buffer re-use
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * block_alloc_size);
	}

	// Clear all blocks and verify we go back down to 0 used memory
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// now we do exactly the same, but with variable-sized blocks
	idx_t variable_block_size = 424242;
	auto alloc_size = BufferManager::GetAllocSize(variable_block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", alloc_size * pin_count)));
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// again, the same but incrementing variable_block_size by 1 for every block (has same alloc_size)
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment variable_block_size
		variable_block_size++;
		CHECK(BufferManager::GetAllocSize(variable_block_size + pin.GetBlockHandle()->GetBlockHeaderSize()) ==
		      alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);

	// reset block size and do the same but decrement by 1 for every block (still same alloc_size)
	variable_block_size = 424242;
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		blocks.push_back(pin.GetBlockHandle());
		CHECK(buffer_manager.GetUsedMemory() == MinValue<idx_t>(pin_count, i + 1) * alloc_size);
		// increment variable_block_size
		variable_block_size--;
		CHECK(BufferManager::GetAllocSize(variable_block_size + pin.GetBlockHandle()->GetBlockHeaderSize()) ==
		      alloc_size);
	}
	for (idx_t i = 0; i < block_count; i++) {
		auto pin = buffer_manager.Pin(blocks[i]);
		CHECK(buffer_manager.GetUsedMemory() == pin_count * alloc_size);
	}
	blocks.clear();
	CHECK(buffer_manager.GetUsedMemory() == 0);
}

TEST_CASE("Test evicted_data not double-decremented for variable-sized blocks", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));
	config->options.maximum_threads = 1;
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);

	idx_t variable_block_size = 424242;
	auto alloc_size = BufferManager::GetAllocSize(variable_block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", alloc_size)));
	REQUIRE_NO_FAIL(con.Query("PRAGMA temp_directory='" + TestCreatePath("eviction_tracking_temp") + "'"));

	shared_ptr<BlockHandle> block_a = nullptr;
	shared_ptr<BlockHandle> block_b = nullptr;
	{
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		block_a = pin.GetBlockHandle();
	}
	{
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, variable_block_size, false);
		block_b = pin.GetBlockHandle();
	}

	// Read block_a back from disk.
	{ auto pin = buffer_manager.Pin(block_a); }

	// Destroy both handles, so remaining temp files are cleaned up.
	block_a.reset();
	block_b.reset();

	// For now there should be no blocks on disk, and evicted_data must be 0.
	for (auto &entry : buffer_manager.GetMemoryUsageInfo()) {
		if (entry.tag == MemoryTag::EXTENSION) {
			CHECK(entry.evicted_data == 0);
		}
	}
}

TEST_CASE("Test buffer allocator", "[storage][.]") {
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();
	config->SetOptionByName("default_block_size", Value::UBIGINT(DEFAULT_BLOCK_ALLOC_SIZE));

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	DuckDB db(storage_database, config.get());
	Connection con(db);

	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	CHECK(buffer_manager.GetUsedMemory() == 0);

	const idx_t limit = 1000000000;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", limit)));

	auto &allocator = buffer_manager.GetBufferAllocator();
	auto block_size = Settings::Get<DefaultBlockSizeSetting>(*config) - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	idx_t requested_size = block_size;
	auto pointer = allocator.AllocateData(requested_size);
	idx_t current_size = requested_size;
	CHECK(buffer_manager.GetUsedMemory() == requested_size);

	// increase
	for (; requested_size < limit; requested_size *= 2) {
		pointer = allocator.ReallocateData(pointer, current_size, requested_size);
		current_size = requested_size;
		CHECK(buffer_manager.GetUsedMemory() == requested_size);
	}

	// decrease
	for (; requested_size >= block_size; requested_size /= 2) {
		pointer = allocator.ReallocateData(pointer, current_size, requested_size);
		current_size = requested_size;
		CHECK(buffer_manager.GetUsedMemory() == requested_size);
	}

	allocator.FreeData(pointer, current_size);
}
