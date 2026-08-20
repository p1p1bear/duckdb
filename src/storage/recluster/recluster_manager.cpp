#include "duckdb/storage/recluster/recluster_manager.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/range_task.hpp"
#include "duckdb/storage/recluster/recluster_candidate.hpp"
#include "duckdb/storage/recluster/recluster_task_context.hpp"
#include "duckdb/storage/recluster/table_recluster_state.hpp"

namespace duckdb {

ReclusterManager::ReclusterManager(AttachedDatabase &db_p) : db(db_p) {
}

uint64_t ReclusterManager::AllocateInitializationToken() {
	auto result = next_initialization_token.fetch_add(1);
	if (result == 0 || result == NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("Recluster table initialization token space is exhausted");
	}
	return result;
}

uint64_t ReclusterManager::BeginCheckpoint() {
	auto result = next_checkpoint_number.fetch_add(1);
	if (result == 0 || result == NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("Recluster checkpoint number space is exhausted");
	}
	return result;
}

shared_ptr<TableReclusterState> ReclusterManager::SynchronizeTable(DuckTableEntry &table) {
	if (!table.HasSortHistory()) {
		return nullptr;
	}
	auto &sort_metadata = *table.GetSortMetadata();
	auto &storage = table.GetStorage();
	auto &table_info = *storage.GetDataTableInfo();
	auto state = table_info.GetReclusterState();
	if (!state) {
		state = table_info.GetOrCreateReclusterState(AllocateInitializationToken());
	}
	auto storage_generation_id = storage.GetRowGroupCollection()->GetStorageGenerationId();
	auto accept_new_tasks = sort_metadata.IsEnabled() && !db.IsReadOnly();
	state->SynchronizeCatalog(sort_metadata.table_id, sort_metadata.current_sort_order_id, storage_generation_id,
	                          accept_new_tasks);

	lock_guard<mutex> guard(queue_lock);
	if (accept_new_tasks) {
		tables[sort_metadata.table_id] = state;
	} else {
		tables.erase(sort_metadata.table_id);
	}
	return state;
}

optional<PendingCheckpointTableState> ReclusterManager::PrepareCheckpoint(DuckTableEntry &table,
                                                                          uint64_t checkpoint_number) {
	if (!table.SortEnabled()) {
		SynchronizeTable(table);
		return nullopt;
	}
	auto state = SynchronizeTable(table);
	auto &storage = table.GetStorage();
	auto snapshot =
	    BuildCheckpointLayoutSnapshot(*storage.GetRowGroupCollection(), storage.Columns(), checkpoint_number);
	if (!snapshot) {
		return nullopt;
	}

	PendingCheckpointTableState result;
	result.table_id = table.GetSortMetadata()->table_id;
	result.sort_order_id = table.GetSortMetadata()->current_sort_order_id;
	result.initialization_token = state->GetInitializationToken();
	result.state = std::move(state);
	result.storage = storage.shared_from_this();
	result.candidate_snapshot = std::move(*snapshot);
	return result;
}

void ReclusterManager::OnCheckpointSuccess(vector<PendingCheckpointTableState> &&states) noexcept {
	for (auto &pending : states) {
		if (!pending.storage || !pending.storage->IsMainTable()) {
			continue;
		}
		auto current_state = pending.storage->GetDataTableInfo()->GetReclusterState();
		if (current_state.get() != pending.state.get() ||
		    current_state->GetInitializationToken() != pending.initialization_token ||
		    current_state->GetTableId() != pending.table_id) {
			continue;
		}
		auto storage_generation_id = pending.storage->GetRowGroupCollection()->GetStorageGenerationId();
		if (storage_generation_id != pending.candidate_snapshot.storage_generation_id) {
			continue;
		}
		pending.state->TryInstallCheckpointSnapshot(pending.sort_order_id, storage_generation_id,
		                                            std::move(pending.candidate_snapshot));
	}
}

template <class CALLBACK>
static void ScanReclusterTables(AttachedDatabase &db, CALLBACK &&callback) {
	auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
	catalog.ScanSchemas([&](SchemaCatalogEntry &schema) {
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal && entry.type == CatalogType::TABLE_ENTRY) {
				callback(entry.Cast<DuckTableEntry>());
			}
		});
	});
}

void ReclusterManager::InitializeCheckpointTables() {
	ScanReclusterTables(db, [&](DuckTableEntry &table) {
		if (!table.SortEnabled()) {
			return;
		}
		auto state = SynchronizeTable(table);
		auto &storage = table.GetStorage();
		auto snapshot = BuildCheckpointLayoutSnapshot(*storage.GetRowGroupCollection(), storage.Columns(), 0);
		if (!snapshot) {
			return;
		}
		state->TryInstallCheckpointSnapshot(table.GetSortMetadata()->current_sort_order_id,
		                                    snapshot->storage_generation_id, std::move(*snapshot));
	});
}

void ReclusterManager::SynchronizeLoadedCatalog() {
	{
		lock_guard<mutex> guard(queue_lock);
		tables.clear();
	}
	ScanReclusterTables(db, [&](DuckTableEntry &table) {
		if (table.HasSortHistory()) {
			SynchronizeTable(table);
		}
	});
}

static vector<idx_t> BindReclusterSortIndexes(const vector<ColumnDefinition> &columns,
                                              const SortOrderDefinition &definition) {
	vector<idx_t> result;
	result.reserve(definition.columns.size());
	for (auto &sort_column : definition.columns) {
		optional_idx physical_index;
		for (idx_t column_index = 0; column_index < columns.size(); column_index++) {
			if (columns[column_index].PersistentColumnId() == sort_column.column_id) {
				physical_index = column_index;
				break;
			}
		}
		if (!physical_index.IsValid()) {
			throw InternalException("Current SORTED BY definition references a missing storage column ID");
		}
		result.push_back(physical_index.GetIndex());
	}
	return result;
}

static recluster_task_id_t GenerateReclusterTaskId(TableReclusterState &state) {
	for (idx_t attempt = 0; attempt < 8; attempt++) {
		auto task_id = UUID::GenerateRandomUUID();
		if (task_id != recluster_task_id_t(0, 0) && !state.GetTask(task_id)) {
			return task_id;
		}
	}
	throw InternalException("Failed to allocate a unique recluster task ID");
}

ReclusterTaskStartResult ReclusterManager::TryStartTask(DuckTableEntry &table, const ReclusterCandidate &candidate) {
	auto &storage = table.GetStorage();
	if (&storage.GetDataTableInfo()->GetDB() != &db) {
		throw InternalException("Cannot start a recluster task through a different attached database");
	}
	auto state = storage.GetDataTableInfo()->GetReclusterState();
	if (!state) {
		return {};
	}

	shared_ptr<RangeTask> task;
	while (true) {
		auto table_write_lock = storage.GetDataTableInfo()->GetExclusiveReclusterWriteLock();
		auto layout_lock = TryGetSharedLayoutPublishLock();
		if (!layout_lock) {
			table_write_lock.reset();
			auto wait_for_checkpoint = GetSharedLayoutPublishLock();
			(void)wait_for_checkpoint;
			continue;
		}

		if (!storage.IsMainTable() || !table.SortEnabled()) {
			return {};
		}
		auto &metadata = *table.GetSortMetadata();
		auto definition = metadata.GetCurrent();
		if (metadata.table_id != state->GetTableId() || !definition ||
		    definition->sort_order_id != candidate.sort_order_id) {
			return {};
		}
		auto validated =
		    RevalidateReclusterCandidate(*storage.GetRowGroupCollection(), storage.Columns(), *state, candidate);
		if (!validated) {
			return {};
		}

		auto physical_sort_indexes = BindReclusterSortIndexes(storage.Columns(), *definition);
		auto task_id = GenerateReclusterTaskId(*state);
		auto task_context = make_uniq<ReclusterTaskContext>(
		    metadata.table_id, state->GetInitializationToken(), std::move(*validated), *definition,
		    std::move(physical_sort_indexes), storage.shared_from_this(), db);
		task = make_shared_ptr<RangeTask>(task_id, std::move(task_context));
		if (!state->TryRegisterTask(task)) {
			return {ReclusterTaskStartStatus::RANGE_UNAVAILABLE, nullptr};
		}
		break;
	}

	if (!task->TryAdvance(RangeTaskState::STARTING, RangeTaskState::PREPARING)) {
		task->TryEnterCancelling();
		task->TryDetach();
		task->GetTaskContext().CloseSnapshot();
		state->RemoveTask(task->GetTaskId());
		return {ReclusterTaskStartStatus::CANCELLED, nullptr};
	}
	return {ReclusterTaskStartStatus::STARTED, std::move(task)};
}

} // namespace duckdb
