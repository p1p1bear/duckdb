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
	REQUIRE(first.Covers(block_manager, {first_block, second_block}));
	REQUIRE(first.IsActiveFor(block_manager));
	REQUIRE(block_manager.IsBlockReserved(first_block));
	REQUIRE(block_manager.IsBlockReserved(second_block));
	REQUIRE(block_manager.PeekFreeBlockId() > second_block);

	auto overlap = AllocatorBlockReservation::Reserve(block_manager, {first_block});
	auto moved = std::move(first);
	REQUIRE(!first.IsActiveFor(block_manager));
	REQUIRE(!first.Covers(block_manager, {}));
	REQUIRE(moved.IsActiveFor(block_manager));
	moved.Release();
	REQUIRE(block_manager.IsBlockReserved(first_block));
	REQUIRE(!block_manager.IsBlockReserved(second_block));
	REQUIRE(block_manager.PeekFreeBlockId() == second_block);

	overlap.Release();
	REQUIRE(!block_manager.IsBlockReserved(first_block));
	REQUIRE(block_manager.PeekFreeBlockId() == first_block);
}

TEST_CASE("Prepared checkpoint block drops preserve multiplicity and have no pre-apply side effects",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_checkpoint_block_drop_multiplicity.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto block_id = block_manager.GetFreeBlockId();
	block_manager.IncreaseBlockReferenceCount(block_id);
	block_manager.IncreaseBlockReferenceCount(block_id);
	auto reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});

	duckdb::vector<block_id_t> too_many_actions {block_id, block_id, block_id, block_id};
	duckdb::vector<CheckpointBlockDropSource> too_many_sources;
	too_many_sources.emplace_back(too_many_actions, too_many_actions, reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(too_many_sources));

	duckdb::vector<block_id_t> duplicate_actions {block_id, block_id};
	duckdb::vector<CheckpointBlockDropSource> duplicate_sources;
	duplicate_sources.emplace_back(duplicate_actions, duplicate_actions, reservation);
	{ auto abandoned_batch = block_manager.PrepareBlockDropsForCheckpoint(duplicate_sources); }

	auto batch = block_manager.PrepareBlockDropsForCheckpoint(duplicate_sources);
	block_manager.ApplyPreparedBlockDrops(std::move(batch));
	reservation.Release();
	block_manager.MarkBlockAsModified(block_id);
	REQUIRE(block_manager.PeekFreeBlockId() == block_id);

	auto partial_block = block_manager.GetFreeBlockId();
	block_manager.IncreaseBlockReferenceCount(partial_block);
	block_manager.IncreaseBlockReferenceCount(partial_block);
	block_manager.IncreaseBlockReferenceCount(partial_block);
	auto partial_reservation = AllocatorBlockReservation::Reserve(block_manager, {partial_block});
	duckdb::vector<block_id_t> partial_actions {partial_block};
	duckdb::vector<CheckpointBlockDropSource> partial_sources;
	partial_sources.emplace_back(partial_actions, partial_actions, partial_reservation);
	auto partial_batch = block_manager.PrepareBlockDropsForCheckpoint(partial_sources);
	block_manager.ApplyPreparedBlockDrops(std::move(partial_batch));
	partial_reservation.Release();
	block_manager.MarkBlockAsModified(partial_block);
	REQUIRE(block_manager.PeekFreeBlockId() != partial_block);
	block_manager.MarkBlockAsModified(partial_block);
	REQUIRE(block_manager.PeekFreeBlockId() != partial_block);
	block_manager.MarkBlockAsModified(partial_block);
	REQUIRE(block_manager.PeekFreeBlockId() == partial_block);
}

TEST_CASE("Prepared committed block drops preserve multiplicity and defer persistent reuse",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_committed_block_drop_multiplicity.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto block_id = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsCheckpointed(block_id);
	block_manager.IncreaseBlockReferenceCount(block_id);
	block_manager.IncreaseBlockReferenceCount(block_id);
	duckdb::vector<block_id_t> too_many_actions {block_id, block_id, block_id, block_id};
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCommit(too_many_actions));

	duckdb::vector<block_id_t> duplicate_actions {block_id, block_id};
	{ auto abandoned_batch = block_manager.PrepareBlockDropsForCommit(duplicate_actions); }
	auto batch = block_manager.PrepareBlockDropsForCommit(duplicate_actions);
	block_manager.ApplyPreparedBlockDrops(std::move(batch));
	block_manager.MarkBlockAsModified(block_id);
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);
	REQUIRE_THROWS(block_manager.MarkBlockAsModified(block_id));
}

TEST_CASE("Prepared committed block drops keep newly used reservations unavailable",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_committed_block_drop_reserved.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto block_id = block_manager.GetFreeBlockId();
	auto reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});
	duckdb::vector<block_id_t> actions {block_id};
	auto batch = block_manager.PrepareBlockDropsForCommit(actions);
	block_manager.ApplyPreparedBlockDrops(std::move(batch));
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);
	reservation.Release();
	REQUIRE(block_manager.PeekFreeBlockId() == block_id);
}

TEST_CASE("Prepared checkpoint block drops release a single reference directly",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_checkpoint_block_drop_single.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto block_id = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsCheckpointed(block_id);
	auto reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});
	duckdb::vector<block_id_t> actions {block_id};
	duckdb::vector<CheckpointBlockDropSource> sources;
	sources.emplace_back(actions, actions, reservation);
	auto batch = block_manager.PrepareBlockDropsForCheckpoint(sources);
	block_manager.ApplyPreparedBlockDrops(std::move(batch));
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);
	reservation.Release();
	REQUIRE(block_manager.PeekFreeBlockId() == block_id);
}

TEST_CASE("Prepared checkpoint block drops aggregate sources and preserve reserved free blocks",
          "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_checkpoint_block_drop_sources.db");
	DeleteDatabase(path);
	DuckDB db(path);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_checkpoint_on_shutdown"));
	REQUIRE_NO_FAIL(con.Query("SET debug_verify_blocks=true"));
	auto &block_manager = GetSingleFileBlockManager(db, con);

	auto block_id = block_manager.GetFreeBlockId();
	block_manager.MarkBlockAsCheckpointed(block_id);
	block_manager.IncreaseBlockReferenceCount(block_id);
	block_manager.IncreaseBlockReferenceCount(block_id);
	auto first_reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});
	auto second_reservation = AllocatorBlockReservation::Reserve(block_manager, {block_id});

	duckdb::vector<block_id_t> first_actions {block_id, block_id};
	duckdb::vector<block_id_t> second_actions {block_id};
	duckdb::vector<CheckpointBlockDropSource> sources;
	sources.emplace_back(first_actions, first_actions, first_reservation);
	sources.emplace_back(second_actions, second_actions, second_reservation);
	auto batch = block_manager.PrepareBlockDropsForCheckpoint(sources);
	block_manager.ApplyPreparedBlockDrops(std::move(batch));

	REQUIRE(block_manager.IsBlockReserved(block_id));
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);
	REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
	REQUIRE_NO_FAIL(con.Query("FORCE CHECKPOINT"));
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);

	first_reservation.Release();
	REQUIRE(block_manager.IsBlockReserved(block_id));
	REQUIRE(block_manager.PeekFreeBlockId() != block_id);
	second_reservation.Release();
	REQUIRE(!block_manager.IsBlockReserved(block_id));
	REQUIRE(block_manager.PeekFreeBlockId() == block_id);
}

TEST_CASE("Prepared checkpoint block drops reject unrelated reservations", "[storage][allocator_block_reservation]") {
	auto path = TestCreatePath("prepared_checkpoint_block_drop_coverage.db");
	auto other_path = TestCreatePath("prepared_checkpoint_block_drop_other.db");
	DeleteDatabase(path);
	DeleteDatabase(other_path);
	DuckDB db(path);
	DuckDB other_db(other_path);
	Connection con(db);
	Connection other_con(other_db);
	auto &block_manager = GetSingleFileBlockManager(db, con);
	auto &other_block_manager = GetSingleFileBlockManager(other_db, other_con);

	auto first_block = block_manager.GetFreeBlockId();
	auto second_block = block_manager.GetFreeBlockId();
	auto third_block = block_manager.GetFreeBlockId();
	auto free_block = CreateFreeBlock(block_manager);
	auto first_reservation = AllocatorBlockReservation::Reserve(block_manager, {first_block});
	auto moved_reservation = std::move(first_reservation);
	duckdb::vector<block_id_t> first_actions {first_block};
	duckdb::vector<CheckpointBlockDropSource> moved_from_sources;
	moved_from_sources.emplace_back(first_actions, first_actions, first_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(moved_from_sources));

	duckdb::vector<block_id_t> uncovered_actions {second_block};
	duckdb::vector<CheckpointBlockDropSource> uncovered_sources;
	uncovered_sources.emplace_back(uncovered_actions, uncovered_actions, moved_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(uncovered_sources));

	duckdb::vector<block_id_t> incomplete_resources {first_block, second_block};
	duckdb::vector<CheckpointBlockDropSource> incomplete_resource_sources;
	incomplete_resource_sources.emplace_back(first_actions, incomplete_resources, moved_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(incomplete_resource_sources));
	moved_reservation.AddPhysicalBlock(second_block);
	{ auto complete_batch = block_manager.PrepareBlockDropsForCheckpoint(incomplete_resource_sources); }

	auto other_reservation = AllocatorBlockReservation::Reserve(other_block_manager, {first_block});
	duckdb::vector<CheckpointBlockDropSource> wrong_manager_sources;
	wrong_manager_sources.emplace_back(first_actions, first_actions, other_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(wrong_manager_sources));

	block_manager.MarkBlockAsCheckpointed(third_block);
	block_manager.MarkBlockAsModified(third_block);
	auto modified_reservation = AllocatorBlockReservation::Reserve(block_manager, {third_block});
	duckdb::vector<block_id_t> modified_actions {third_block};
	duckdb::vector<CheckpointBlockDropSource> modified_sources;
	modified_sources.emplace_back(modified_actions, modified_actions, modified_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(modified_sources));
	auto free_reservation = AllocatorBlockReservation::Reserve(block_manager, {free_block});
	duckdb::vector<block_id_t> free_actions {free_block};
	duckdb::vector<CheckpointBlockDropSource> free_sources;
	free_sources.emplace_back(free_actions, free_actions, free_reservation);
	REQUIRE_THROWS(block_manager.PrepareBlockDropsForCheckpoint(free_sources));

	moved_reservation.Release();
	other_reservation.Release();
	modified_reservation.Release();
	free_reservation.Release();
	block_manager.MarkBlockAsModified(first_block);
	block_manager.MarkBlockAsModified(second_block);
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
