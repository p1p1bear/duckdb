#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/allocator_block_reservation.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <exception>
#include <thread>

using namespace duckdb; // NOLINT

static SingleFileBlockManager &GetSingleFileBlockManager(DuckDB &db, Connection &con) {
	auto database_name = DatabaseManager::GetDefaultDatabase(*con.context);
	auto attached = db.instance->GetDatabaseManager().GetDatabase(database_name);
	REQUIRE(attached);
	return attached->GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
}

static block_id_t CreateFreeBlock(SingleFileBlockManager &block_manager) {
	auto block_id = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsModified(block_id);
	return block_id;
}

TEST_CASE("Allocator block reservations prevent overlapping physical reuse", "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_block_reservation.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto first_block = block_manager.GetFreeBlockId();
	auto second_block = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsModified(first_block);
	block_manager.MarkBlockAsModified(second_block);
	REQUIRE(block_manager.PeekFreeBlockId() == first_block);
	REQUIRE_THROWS(AllocatorBlockReservation::Reserve(block_manager, {first_block, MAXIMUM_BLOCK}));
	REQUIRE(!block_manager.IsBlockReserved(first_block));
	REQUIRE(block_manager.PeekFreeBlockId() == first_block);

	auto first = AllocatorBlockReservation::Reserve(block_manager, {first_block, first_block});
	first.AddPhysicalBlock(second_block);
	REQUIRE(first.Covers({first_block, second_block}));
	REQUIRE(block_manager.IsBlockReserved(first_block));
	REQUIRE(block_manager.IsBlockReserved(second_block));
	REQUIRE(block_manager.PeekFreeBlockId() > second_block);

	auto overlap = AllocatorBlockReservation::Reserve(block_manager, {first_block});
	auto moved = std::move(first);
	moved.Release();
	REQUIRE(block_manager.IsBlockReserved(first_block));
	REQUIRE(!block_manager.IsBlockReserved(second_block));
	REQUIRE(block_manager.PeekFreeBlockId() == second_block);

	overlap.Release();
	REQUIRE(!block_manager.IsBlockReserved(first_block));
	REQUIRE(block_manager.PeekFreeBlockId() == first_block);
}

TEST_CASE("Allocator block reservations preserve transferred and future ownership",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_block_reservation_transfer.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto published_block = CreateFreeBlock(block_manager);
	auto published = AllocatorBlockReservation::Reserve(block_manager, {published_block});
	block_manager.MarkBlockAsUsed(published_block);
	published.Release();
	REQUIRE(block_manager.PeekFreeBlockId() != published_block);
	block_manager.MarkBlockAsModified(published_block);

	auto previous_block_count = block_manager.TotalBlocks();
	auto future_block = NumericCast<block_id_t>(previous_block_count + 3);
	auto future = AllocatorBlockReservation::Reserve(block_manager, {future_block});
	REQUIRE(block_manager.TotalBlocks() == NumericCast<idx_t>(future_block + 1));
	block_manager.Truncate();
	REQUIRE(block_manager.TotalBlocks() == NumericCast<idx_t>(future_block + 1));
	future.Release();
	block_manager.Truncate();
	REQUIRE(block_manager.TotalBlocks() == previous_block_count);
}

TEST_CASE("Allocator block reservations protect metadata sub-block allocation",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_metadata_reservation.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);
	auto &metadata_manager = block_manager.GetMetadataManager();

	block_id_t first_block;
	{
		auto first = metadata_manager.AllocateHandle();
		first_block = NumericCast<block_id_t>(first.pointer.block_index);
	}
	REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
	auto reservation = AllocatorBlockReservation::Reserve(block_manager, {first_block});
	auto second = metadata_manager.AllocateHandle();
	REQUIRE(NumericCast<block_id_t>(second.pointer.block_index) != first_block);
}

TEST_CASE("Task-private metadata reservations remain checkpoint-safe", "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_metadata_concurrency.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
	auto &block_manager = GetSingleFileBlockManager(db, con);
	auto &metadata_manager = block_manager.GetMetadataManager();

	auto owner = metadata_manager.CreateTaskPrivateBlockOwner();
	block_id_t first_block;
	{
		auto first = owner->AllocateHandle();
		first_block = NumericCast<block_id_t>(first.pointer.block_index);
	}
	auto reservation = AllocatorBlockReservation::Reserve(block_manager, {first_block});

	std::atomic<bool> reused_reserved_block(false);
	std::atomic<idx_t> ready(0);
	std::atomic<bool> start(false);
	std::exception_ptr allocation_error;
	std::exception_ptr checkpoint_error;
	Connection checkpoint_connection(db);
	std::thread allocator([&]() {
		try {
			ready++;
			while (!start.load()) {
				std::this_thread::yield();
			}
			duckdb::vector<MetadataHandle> handles;
			for (idx_t i = 0; i < MetadataManager::METADATA_BLOCK_COUNT * 4; i++) {
				auto handle = owner->AllocateHandle();
				if (NumericCast<block_id_t>(handle.pointer.block_index) == first_block) {
					reused_reserved_block = true;
				}
				handles.push_back(std::move(handle));
				if (i % MetadataManager::METADATA_BLOCK_COUNT == 0) {
					std::this_thread::yield();
				}
			}
		} catch (...) {
			allocation_error = std::current_exception();
		}
	});
	std::thread checkpointer([&]() {
		try {
			ready++;
			while (!start.load()) {
				std::this_thread::yield();
			}
			for (idx_t i = 0; i < 4; i++) {
				auto result = checkpoint_connection.Query("FORCE CHECKPOINT");
				if (result->HasError()) {
					throw InternalException("Concurrent checkpoint failed: %s", result->GetError());
				}
			}
		} catch (...) {
			checkpoint_error = std::current_exception();
		}
	});
	while (ready.load() != 2) {
		std::this_thread::yield();
	}
	start = true;
	allocator.join();
	checkpointer.join();

	if (allocation_error) {
		std::rethrow_exception(allocation_error);
	}
	if (checkpoint_error) {
		std::rethrow_exception(checkpoint_error);
	}
	REQUIRE(!reused_reserved_block);
	owner->Abort();
	reservation.Release();
	REQUIRE(block_manager.PeekFreeBlockId() == first_block);
}

TEST_CASE("Published and recovered metadata blocks participate in the next checkpoint",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("published_metadata_checkpoint.db");
	DeleteDatabase(path);
	MetaBlockPointer pointer;
	block_id_t block_id;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &block_manager = GetSingleFileBlockManager(db, con);
		auto &metadata_manager = block_manager.GetMetadataManager();
		auto owner = metadata_manager.CreateTaskPrivateBlockOwner();
		{
			auto handle = owner->AllocateHandle();
			pointer = metadata_manager.GetDiskPointer(handle.pointer);
			block_id = pointer.GetBlockId();
		}
		owner->Flush();
		owner->MarkPublished();
		REQUIRE(metadata_manager.BlockIsModified(pointer));
		metadata_manager.ClearModifiedBlocks({pointer});
		REQUIRE(metadata_manager.BlockHasBeenCleared(pointer));
		block_manager.FileSync();
	}
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		auto &block_manager = GetSingleFileBlockManager(db, con);
		auto reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});
		auto &metadata_manager = block_manager.GetMetadataManager();
		metadata_manager.RegisterRecoveryDiskPointer(pointer);
		REQUIRE(metadata_manager.BlockIsModified(pointer));
		metadata_manager.ClearModifiedBlocks({pointer});
		REQUIRE(metadata_manager.BlockHasBeenCleared(pointer));
	}
}

TEST_CASE("Concurrent metadata conversion preserves allocated payloads", "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_metadata_conversion.db");
	DeleteDatabase(path);
	static constexpr idx_t thread_count = 16;
	duckdb::vector<MetaBlockPointer> pointers(thread_count);
	duckdb::vector<uint64_t> expected(thread_count);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);
	auto &metadata_manager = block_manager.GetMetadataManager();
	auto first = metadata_manager.AllocateHandle();
	auto first_block = NumericCast<block_id_t>(first.pointer.block_index);
	REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));

	std::atomic<idx_t> ready(0);
	std::atomic<bool> start(false);
	duckdb::vector<std::exception_ptr> errors(thread_count);
	duckdb::vector<std::thread> workers;
	for (idx_t thread_index = 0; thread_index < thread_count; thread_index++) {
		workers.emplace_back([&, thread_index]() {
			try {
				ready++;
				while (!start.load()) {
					std::this_thread::yield();
				}
				auto handle = metadata_manager.AllocateHandle();
				auto block_id = NumericCast<block_id_t>(handle.pointer.block_index);
				if (block_id != first_block) {
					throw InternalException("Concurrent metadata allocation selected block %d instead of %d", block_id,
					                        first_block);
				}
				expected[thread_index] = 0xA110C000ULL + thread_index;
				pointers[thread_index] = metadata_manager.GetDiskPointer(handle.pointer);
				auto offset = handle.pointer.index * metadata_manager.GetMetadataBlockSize();
				Store<uint64_t>(expected[thread_index], handle.handle.GetDataMutable() + offset);
			} catch (...) {
				errors[thread_index] = std::current_exception();
			}
		});
	}
	while (ready.load() != thread_count) {
		std::this_thread::yield();
	}
	start = true;
	for (auto &worker : workers) {
		worker.join();
	}
	for (auto &error : errors) {
		if (error) {
			std::rethrow_exception(error);
		}
	}

	metadata_manager.Flush();
	block_manager.FileSync();
	auto disk_block = block_manager.CreateBlock(first_block, nullptr);
	block_manager.Read(QueryContext(), *disk_block);
	for (idx_t thread_index = 0; thread_index < thread_count; thread_index++) {
		auto offset = pointers[thread_index].GetBlockIndex() * metadata_manager.GetMetadataBlockSize();
		REQUIRE(Load<uint64_t>(disk_block->GetData() + offset) == expected[thread_index]);
	}
}

TEST_CASE("Allocator block reservations survive consecutive checkpoint free lists",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("allocator_checkpoint_reservation.db");
	DeleteDatabase(path);
	block_id_t reserved_block;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
		REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
		auto &block_manager = GetSingleFileBlockManager(db, con);
		reserved_block = CreateFreeBlock(block_manager);
		auto reservation = AllocatorBlockReservation::Reserve(block_manager, {reserved_block});

		block_manager.Truncate();
		REQUIRE(block_manager.TotalBlocks() > NumericCast<idx_t>(reserved_block));
		REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
		REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
		reservation.Release();
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto &block_manager = GetSingleFileBlockManager(db, con);
		REQUIRE(block_manager.PeekFreeBlockId() == reserved_block);
	}
}
