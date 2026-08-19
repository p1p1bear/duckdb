#include "catch.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace duckdb;

TEST_CASE("StorageLock try-shared respects queued writers", "[storage_lock]") {
	StorageLock storage_lock;
	auto initial_reader = storage_lock.GetSharedLock();

	std::mutex writer_lock;
	std::condition_variable writer_cv;
	bool writer_started = false;
	bool release_writer = false;
	std::atomic<bool> writer_acquired(false);

	std::thread writer([&]() {
		{
			std::lock_guard<std::mutex> guard(writer_lock);
			writer_started = true;
		}
		writer_cv.notify_one();

		auto exclusive_lock = storage_lock.GetExclusiveLock();
		writer_acquired = true;
		writer_cv.notify_one();

		std::unique_lock<std::mutex> guard(writer_lock);
		writer_cv.wait(guard, [&]() { return release_writer; });
	});

	{
		std::unique_lock<std::mutex> guard(writer_lock);
		writer_cv.wait(guard, [&]() { return writer_started; });
	}

	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (storage_lock.TryGetSharedLock()) {
		if (std::chrono::steady_clock::now() >= deadline) {
			initial_reader.reset();
			{
				std::lock_guard<std::mutex> guard(writer_lock);
				release_writer = true;
			}
			writer_cv.notify_one();
			writer.join();
			FAIL("Queued writer was bypassed by try-shared acquisition");
		}
		std::this_thread::yield();
	}

	initial_reader.reset();
	{
		std::unique_lock<std::mutex> guard(writer_lock);
		writer_cv.wait(guard, [&]() { return writer_acquired.load(); });
	}
	REQUIRE(!storage_lock.TryGetSharedLock());

	{
		std::lock_guard<std::mutex> guard(writer_lock);
		release_writer = true;
	}
	writer_cv.notify_one();
	writer.join();

	REQUIRE(storage_lock.TryGetSharedLock());
}
