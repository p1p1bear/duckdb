#include "catch.hpp"
#include "duckdb/storage/recluster/range_task.hpp"

#include <algorithm>
#include <thread>

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<RangeTask> MakeDeleteJournalTask(ReclusterDeleteJournalLimits limits = {16, 32}) {
	return duckdb::make_shared_ptr<RangeTask>(hugeint_t(0, 1), RowGroupRange {100, 200}, limits);
}

TEST_CASE("Recluster DELETE journal exposes only a resolved prefix", "[storage][recluster_delete_journal]") {
	auto task = MakeDeleteJournalTask();
	auto first = task->TryReserveDeleteSlot({101, 103});
	auto second = task->TryReserveDeleteSlot({105});
	auto third = task->TryReserveDeleteSlot({107, 109});
	REQUIRE(first);
	REQUIRE(second);
	REQUIRE(third);
	REQUIRE(first->GetSequence() == 1);
	REQUIRE(second->GetSequence() == 2);
	REQUIRE(third->GetSequence() == 3);

	REQUIRE(task->ResolveDeleteSlot(*second, DeleteSlotState::COMMITTED));
	REQUIRE(task->ResolveDeleteSlot(*third, DeleteSlotState::ABORTED));
	auto blocked = task->ScanResolvedDeletes(0, 16, 32);
	REQUIRE(blocked.slots.empty());
	REQUIRE(blocked.resolved_through == 0);
	REQUIRE(blocked.blocked_by_reserved);
	REQUIRE(!blocked.limit_exceeded);

	REQUIRE(task->ResolveDeleteSlot(*first, DeleteSlotState::COMMITTED));
	REQUIRE(task->ResolveDeleteSlot(*first, DeleteSlotState::COMMITTED));
	REQUIRE(!task->ResolveDeleteSlot(*first, DeleteSlotState::ABORTED));
	auto resolved = task->ScanResolvedDeletes(0, 16, 32);
	REQUIRE(resolved.slots.size() == 3);
	REQUIRE(resolved.resolved_through == 3);
	REQUIRE(resolved.committed_rowid_count == 3);
	REQUIRE(!resolved.blocked_by_reserved);
	REQUIRE(!resolved.limit_exceeded);
	REQUIRE(resolved.slots[0].get().GetState() == DeleteSlotState::COMMITTED);
	REQUIRE(resolved.slots[1].get().GetState() == DeleteSlotState::COMMITTED);
	REQUIRE(resolved.slots[2].get().GetState() == DeleteSlotState::ABORTED);

	auto tail = task->ScanResolvedDeletes(1, 16, 32);
	REQUIRE(tail.slots.size() == 2);
	REQUIRE(tail.resolved_through == 3);
	REQUIRE(tail.committed_rowid_count == 1);
}

TEST_CASE("Recluster DELETE journal enforces task and scan limits", "[storage][recluster_delete_journal]") {
	auto task = MakeDeleteJournalTask({2, 3});
	REQUIRE(!task->TryReserveDeleteSlot({99}));
	REQUIRE(!task->TryReserveDeleteSlot({200}));

	auto first = task->TryReserveDeleteSlot({101, 102});
	REQUIRE(first);
	REQUIRE(!task->TryReserveDeleteSlot({103, 104}));
	auto second = task->TryReserveDeleteSlot({105});
	REQUIRE(second);
	REQUIRE(!task->TryReserveDeleteSlot({106}));
	REQUIRE(task->GetLatestDeleteSequence() == 2);
	REQUIRE(task->ResolveDeleteSlot(*first, DeleteSlotState::COMMITTED));
	REQUIRE(task->ResolveDeleteSlot(*second, DeleteSlotState::COMMITTED));

	auto slot_limited = task->ScanResolvedDeletes(0, 1, 3);
	REQUIRE(slot_limited.slots.size() == 1);
	REQUIRE(slot_limited.resolved_through == 1);
	REQUIRE(slot_limited.limit_exceeded);
	auto row_limited = task->ScanResolvedDeletes(0, 2, 2);
	REQUIRE(row_limited.slots.size() == 1);
	REQUIRE(row_limited.committed_rowid_count == 2);
	REQUIRE(row_limited.limit_exceeded);
}

TEST_CASE("Recluster DELETE journal keeps slot addresses stable", "[storage][recluster_delete_journal]") {
	auto task = MakeDeleteJournalTask({100, 100});
	duckdb::vector<optional_ptr<ReclusterDeleteSlot>> slots;
	for (row_t row_id = 100; row_id < 200; row_id++) {
		auto slot = task->TryReserveDeleteSlot({row_id});
		REQUIRE(slot);
		slots.push_back(slot);
	}

	std::atomic<bool> all_resolved(true);
	std::thread evens([&]() {
		for (idx_t i = 0; i < slots.size(); i += 2) {
			if (!task->ResolveDeleteSlot(*slots[i], DeleteSlotState::COMMITTED)) {
				all_resolved = false;
			}
		}
	});
	std::thread odds([&]() {
		for (idx_t i = 1; i < slots.size(); i += 2) {
			if (!task->ResolveDeleteSlot(*slots[i], DeleteSlotState::ABORTED)) {
				all_resolved = false;
			}
		}
	});
	evens.join();
	odds.join();
	REQUIRE(all_resolved);

	auto resolved = task->ScanResolvedDeletes(0, 100, 100);
	REQUIRE(resolved.slots.size() == 100);
	REQUIRE(resolved.resolved_through == 100);
	REQUIRE(resolved.committed_rowid_count == 50);
	for (idx_t i = 0; i < slots.size(); i++) {
		REQUIRE(slots[i]->GetSequence() == i + 1);
	}
}

TEST_CASE("Recluster DELETE journal serializes concurrent reservations", "[storage][recluster_delete_journal]") {
	auto task = MakeDeleteJournalTask({100, 100});
	duckdb::vector<duckdb::vector<optional_ptr<ReclusterDeleteSlot>>> thread_slots(10);
	std::atomic<bool> all_reserved(true);
	duckdb::vector<std::thread> workers;
	for (idx_t thread_index = 0; thread_index < thread_slots.size(); thread_index++) {
		workers.emplace_back([&, thread_index]() {
			for (idx_t row_index = 0; row_index < 10; row_index++) {
				auto row_id = NumericCast<row_t>(100 + thread_index * 10 + row_index);
				auto slot = task->TryReserveDeleteSlot({row_id});
				if (!slot) {
					all_reserved = false;
					continue;
				}
				thread_slots[thread_index].push_back(slot);
			}
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	REQUIRE(all_reserved);
	REQUIRE(task->GetLatestDeleteSequence() == 100);

	duckdb::vector<delete_sequence_t> sequences;
	for (auto &slots : thread_slots) {
		REQUIRE(slots.size() == 10);
		for (auto slot : slots) {
			sequences.push_back(slot->GetSequence());
			REQUIRE(task->ResolveDeleteSlot(*slot, DeleteSlotState::COMMITTED));
		}
	}
	std::sort(sequences.begin(), sequences.end());
	for (idx_t sequence_index = 0; sequence_index < sequences.size(); sequence_index++) {
		REQUIRE(sequences[sequence_index] == sequence_index + 1);
	}
	REQUIRE(task->ScanResolvedDeletes(0, 100, 100).resolved_through == 100);
}

TEST_CASE("Recluster DELETE slots cannot be resolved through another task", "[storage][recluster_delete_journal]") {
	auto first_task = MakeDeleteJournalTask();
	auto second_task = duckdb::make_shared_ptr<RangeTask>(hugeint_t(0, 2), RowGroupRange {100, 200},
	                                                      ReclusterDeleteJournalLimits {16, 32});
	auto slot = first_task->TryReserveDeleteSlot({123});
	REQUIRE(slot);
	REQUIRE(!second_task->ResolveDeleteSlot(*slot, DeleteSlotState::COMMITTED));
	REQUIRE(slot->GetState() == DeleteSlotState::RESERVED);
	REQUIRE(first_task->ResolveDeleteSlot(*slot, DeleteSlotState::COMMITTED));
}
