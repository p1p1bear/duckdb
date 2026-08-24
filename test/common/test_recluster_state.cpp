#include "catch.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"

#include <thread>

using namespace duckdb; // NOLINT

static duckdb::shared_ptr<RangeTask> MakeRangeTask(uint64_t task_id, row_t start, row_t end) {
	return duckdb::make_shared_ptr<RangeTask>(hugeint_t(0, task_id), RowGroupRange {start, end});
}

static void AdvanceToFinalizing(RangeTask &task) {
	REQUIRE(task.TryAdvance(RangeTaskState::STARTING, RangeTaskState::PREPARING));
	REQUIRE(task.TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	REQUIRE(task.TryAdvance(RangeTaskState::CATCHING_UP_DELETES, RangeTaskState::PREPARED));
	REQUIRE(task.TryAdvance(RangeTaskState::PREPARED, RangeTaskState::FINALIZING));
}

TEST_CASE("Range task control word preserves cancellation and publication flags", "[storage][recluster_state]") {
	auto task = MakeRangeTask(1, 0, 100);
	REQUIRE(task->GetState() == RangeTaskState::STARTING);
	REQUIRE(!task->IsCancelRequested());
	REQUIRE(!task->IsPublishForbidden());
	REQUIRE(!task->IsFinished());

	REQUIRE(task->TryAdvance(RangeTaskState::STARTING, RangeTaskState::PREPARING));
	task->DisablePublishForJournalFailure();
	task->RequestCancel();
	REQUIRE(task->IsPublishForbidden());
	REQUIRE(task->IsCancelRequested());
	REQUIRE(!task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	REQUIRE(task->TryEnterCancelling());
	REQUIRE(task->GetState() == RangeTaskState::CANCELLING);
	REQUIRE(task->TryDetach());
	REQUIRE(task->GetState() == RangeTaskState::DETACHED);
	REQUIRE(task->IsFinished());
	REQUIRE(task->IsCancelRequested());
	REQUIRE(task->IsPublishForbidden());
}

TEST_CASE("Range task cancellation races atomically with commit entry", "[storage][recluster_state]") {
	for (idx_t iteration = 0; iteration < 200; iteration++) {
		auto task = MakeRangeTask(iteration + 1, 0, 100);
		AdvanceToFinalizing(*task);

		bool entered_commit = false;
		std::thread committer([&]() { entered_commit = task->TryEnterCommitting(); });
		std::thread canceller([&]() { task->RequestCancel(); });
		committer.join();
		canceller.join();

		REQUIRE(task->IsCancelRequested());
		if (entered_commit) {
			REQUIRE(task->GetState() == RangeTaskState::COMMITTING);
			REQUIRE(task->TryFinishCommit(true));
			REQUIRE(task->GetState() == RangeTaskState::PUBLISHED);
			REQUIRE(task->IsCancelRequested());
		} else {
			REQUIRE(task->GetState() == RangeTaskState::FINALIZING);
			REQUIRE(task->TryEnterCancelling());
			REQUIRE(task->TryDetach());
		}
		REQUIRE(task->IsFinished());
	}
}

TEST_CASE("Table recluster state reserves non-overlapping ranges", "[storage][recluster_state]") {
	TableReclusterState state(42);
	REQUIRE(state.GetInitializationToken() == 42);
	REQUIRE(!state.AcceptsNewTasks());
	REQUIRE(!state.TryRegisterTask(MakeRangeTask(1, 0, 10)));

	state.SetAcceptNewTasks(true);
	auto first = MakeRangeTask(1, 0, 10);
	auto adjacent = MakeRangeTask(2, 10, 20);
	REQUIRE(state.TryRegisterTask(first));
	REQUIRE(state.TryRegisterTask(adjacent));
	REQUIRE(!state.TryRegisterTask(MakeRangeTask(3, 5, 15)));
	REQUIRE(!state.TryRegisterTask(MakeRangeTask(1, 20, 30)));
	REQUIRE(state.GetTask(1).get() == first.get());

	auto ranges = state.GetReservedRanges();
	REQUIRE(ranges.size() == 2);
	REQUIRE(ranges[0].start == 0);
	REQUIRE(ranges[0].end == 10);
	REQUIRE(ranges[1].start == 10);
	REQUIRE(ranges[1].end == 20);

	state.RemoveTask(1);
	REQUIRE(!state.GetTask(1));
	REQUIRE(state.TryRegisterTask(MakeRangeTask(3, 0, 10)));
	auto tasks = state.DisableAndGetTasks();
	REQUIRE(tasks.size() == 2);
	REQUIRE(!state.AcceptsNewTasks());
	REQUIRE(!state.TryRegisterTask(MakeRangeTask(4, 20, 30)));
}

TEST_CASE("Table recluster state installs snapshots for the active catalog generation", "[storage][recluster_state]") {
	TableReclusterState state(99);
	auto table_id = hugeint_t(7, 11);
	state.SynchronizeCatalog(table_id, 3, 17, true);
	REQUIRE(state.AcceptsNewTasks());
	REQUIRE(state.GetTableId() == table_id);
	REQUIRE(state.GetCurrentSortOrderId() == 3);
	REQUIRE(state.GetCurrentStorageGenerationId() == 17);
	REQUIRE(!state.GetLastCheckpoint());

	CheckpointLayoutSnapshot snapshot;
	snapshot.checkpoint_number = 8;
	snapshot.storage_generation_id = 17;
	REQUIRE(!state.TryInstallCheckpointSnapshot(4, 17, snapshot));
	REQUIRE(!state.TryInstallCheckpointSnapshot(3, 18, snapshot));
	REQUIRE(state.TryInstallCheckpointSnapshot(3, 17, snapshot));
	REQUIRE(state.GetLastCheckpoint() == snapshot);

	state.SynchronizeCatalog(table_id, 3, 17, true);
	REQUIRE(state.GetLastCheckpoint() == snapshot);
	state.SynchronizeCatalog(table_id, 3, 18, true);
	REQUIRE(!state.GetLastCheckpoint());
	state.SynchronizeCatalog(table_id, INVALID_SORT_ORDER_ID, 18, true);
	REQUIRE(!state.AcceptsNewTasks());
	REQUIRE_THROWS_AS(state.SynchronizeCatalog(hugeint_t(8, 11), 3, 18, true), InternalException);
}

TEST_CASE("Table recluster state snapshots task metrics and observations", "[storage][recluster_state]") {
	TableReclusterState state(101);
	state.SetAcceptNewTasks(true);
	auto task = MakeRangeTask(1, 0, 100);
	auto first = task->TryReserveDeleteSlot({1, 2});
	auto second = task->TryReserveDeleteSlot({3});
	auto aborted = task->TryReserveDeleteSlot({4, 5});
	REQUIRE(first);
	REQUIRE(second);
	REQUIRE(aborted);
	REQUIRE(task->ResolveDeleteSlot(*first, DeleteSlotState::COMMITTED));
	REQUIRE(task->ResolveDeleteSlot(*second, DeleteSlotState::COMMITTED));
	REQUIRE(task->ResolveDeleteSlot(*aborted, DeleteSlotState::ABORTED));
	REQUIRE(state.TryRegisterTask(task));

	auto status = state.GetTaskStatus();
	REQUIRE(status.active_prepare_tasks == 1);
	REQUIRE(status.pending_finalize_tasks == 0);
	REQUIRE(status.pending_delete_rows == 3);
	REQUIRE(status.prepared_bytes == 0);

	task->UpdatePreparedOutputStatus(first->GetSequence(), 8192);
	REQUIRE(task->TryAdvance(RangeTaskState::STARTING, RangeTaskState::PREPARING));
	REQUIRE(task->TryAdvance(RangeTaskState::PREPARING, RangeTaskState::CATCHING_UP_DELETES));
	REQUIRE(task->TryAdvance(RangeTaskState::CATCHING_UP_DELETES, RangeTaskState::PREPARED));
	status = state.GetTaskStatus();
	REQUIRE(status.active_prepare_tasks == 0);
	REQUIRE(status.pending_finalize_tasks == 1);
	REQUIRE(status.pending_delete_rows == 1);
	REQUIRE(status.prepared_bytes == 8192);

	auto age = state.ObserveRemainingWorkAgeMs(true);
	REQUIRE(age);
	REQUIRE(*age >= 0);
	REQUIRE(!state.ObserveRemainingWorkAgeMs(false));
	state.SetLastError("test failure");
	REQUIRE(state.GetLastError() == "test failure");
}
