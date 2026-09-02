#include "duckdb/storage/recluster/recluster_wal_replay.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/allocator_block_reservation.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/recluster/checkpoint_snapshot.hpp"
#include "duckdb/storage/recluster/recluster_commit.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/replacement_manifest.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

enum class ReclusterWALReplayPhase : uint8_t { PRESCAN, REPLAY };

enum class ReclusterWALTransactionError : uint8_t { NONE, DELETE_WITHOUT_HEADER, MULTIPLE_HEADERS, MIXED_TRANSACTION };

struct PendingReclusterWALTransaction {
	optional<WALReclusterEntry> header;
	vector<WALReclusterDeleteEntry> deletes;
	vector<idx_t> reservation_indexes;
	ReclusterWALTransactionError error = ReclusterWALTransactionError::NONE;
	bool has_other_write = false;
	bool catalog_changed = false;

	bool HasReclusterEntries() const {
		return header.has_value() || !deletes.empty() || error != ReclusterWALTransactionError::NONE;
	}

	void Reset() {
		header.reset();
		deletes.clear();
		reservation_indexes.clear();
		error = ReclusterWALTransactionError::NONE;
		has_other_write = false;
		catalog_changed = false;
	}
};

static bool WALTypeChangesCatalog(WALType type) {
	switch (type) {
	case WALType::CREATE_TABLE:
	case WALType::DROP_TABLE:
	case WALType::CREATE_SCHEMA:
	case WALType::DROP_SCHEMA:
	case WALType::CREATE_VIEW:
	case WALType::DROP_VIEW:
	case WALType::CREATE_SEQUENCE:
	case WALType::DROP_SEQUENCE:
	case WALType::CREATE_MACRO:
	case WALType::DROP_MACRO:
	case WALType::CREATE_TYPE:
	case WALType::DROP_TYPE:
	case WALType::ALTER_INFO:
	case WALType::CREATE_TABLE_MACRO:
	case WALType::DROP_TABLE_MACRO:
	case WALType::CREATE_INDEX:
	case WALType::DROP_INDEX:
	case WALType::CREATE_TRIGGER:
	case WALType::DROP_TRIGGER:
		return true;
	default:
		return false;
	}
}

struct ReclusterWALReplayRecord {
	WALReclusterEntry header;
	ReplacementManifest manifest;
	vector<row_t> final_deleted_new_rowids;
	vector<block_id_t> manifest_blocks;
	vector<block_id_t> replacement_blocks;
	ReclusterWALRetentionReservation wal_reservation;
	ReclusterWALPosition transaction_end;
	optional<string> validation_error;
	bool committed = false;
};

class ProtectedManifestReader : public ReadStream {
public:
	ProtectedManifestReader(BlockManager &block_manager_p, MetaBlockPointer pointer_p,
	                        vector<block_id_t> &protected_blocks_p, AllocatorBlockReservation &reservation_p)
	    : block_manager(block_manager_p),
	      metadata_block_size(block_manager.GetMetadataManager().GetMetadataBlockSize()), next_pointer(pointer_p),
	      protected_blocks(protected_blocks_p), reservation(reservation_p), has_next_block(true), offset(0),
	      next_offset(pointer_p.offset), capacity(0) {
	}

	void ReadData(data_ptr_t target, idx_t read_size) override {
		ReadData(QueryContext(), target, read_size);
	}

	void ReadData(QueryContext context, data_ptr_t target, idx_t read_size) override {
		while (offset + read_size > capacity) {
			auto available = capacity - offset;
			if (available > 0) {
				memcpy(target, Ptr(), available);
				target += available;
				read_size -= available;
				offset += available;
			}
			ReadNextBlock(context);
		}
		memcpy(target, Ptr(), read_size);
		offset += read_size;
	}

private:
	void ProtectBlock(block_id_t block_id) {
		if (!block_manager.Cast<SingleFileBlockManager>().BlockExistsOnDisk(block_id)) {
			throw DataCorruptionException("Recluster manifest references block %d outside the database file", block_id);
		}
		if (std::find(protected_blocks.begin(), protected_blocks.end(), block_id) != protected_blocks.end()) {
			return;
		}
		reservation.AddPhysicalBlock(block_id);
		protected_blocks.push_back(block_id);
	}

	void ReadNextBlock(QueryContext context) {
		if (!has_next_block) {
			throw DataCorruptionException("Recluster manifest metadata chain ended before its payload");
		}
		if (!next_pointer.IsValid() || next_pointer.GetBlockIndex() >= MetadataManager::METADATA_BLOCK_COUNT ||
		    std::find(visited_pointers.begin(), visited_pointers.end(), next_pointer.block_pointer) !=
		        visited_pointers.end()) {
			throw DataCorruptionException("Recluster manifest contains an invalid metadata chain");
		}
		visited_pointers.push_back(next_pointer.block_pointer);
		ProtectBlock(next_pointer.GetBlockId());

		auto block_handle = block_manager.RegisterBlock(next_pointer.GetBlockId());
		block = block_manager.GetBufferManager().Pin(context, block_handle);
		block_index = next_pointer.GetBlockIndex();
		auto next_block = Load<idx_t>(BasePtr());
		if (next_block == idx_t(-1)) {
			has_next_block = false;
		} else {
			next_pointer = MetaBlockPointer(next_block, 0);
		}
		if (next_offset < sizeof(block_id_t)) {
			next_offset = sizeof(block_id_t);
		}
		if (next_offset > metadata_block_size) {
			throw DataCorruptionException("Recluster manifest metadata offset exceeds its block");
		}
		offset = next_offset;
		next_offset = sizeof(block_id_t);
		capacity = metadata_block_size;
	}

	const_data_ptr_t BasePtr() {
		return block.Ptr() + block_index * metadata_block_size;
	}

	const_data_ptr_t Ptr() {
		return BasePtr() + offset;
	}

private:
	BlockManager &block_manager;
	idx_t metadata_block_size;
	MetaBlockPointer next_pointer;
	vector<block_id_t> &protected_blocks;
	AllocatorBlockReservation &reservation;
	vector<idx_t> visited_pointers;
	BufferHandle block;
	uint32_t block_index = 0;
	bool has_next_block;
	idx_t offset;
	idx_t next_offset;
	idx_t capacity;
};

static void ThrowTransactionError(ReclusterWALTransactionError error) {
	switch (error) {
	case ReclusterWALTransactionError::DELETE_WITHOUT_HEADER:
		throw DataCorruptionException("Recluster WAL DELETE appears before its header");
	case ReclusterWALTransactionError::MULTIPLE_HEADERS:
		throw DataCorruptionException("A WAL transaction contains multiple recluster headers");
	case ReclusterWALTransactionError::MIXED_TRANSACTION:
		throw DataCorruptionException("A recluster WAL transaction contains another write operation");
	case ReclusterWALTransactionError::NONE:
		return;
	}
	throw InternalException("Unknown recluster WAL transaction error");
}

static vector<row_t> ValidateWALTransaction(const PendingReclusterWALTransaction &pending) {
	ThrowTransactionError(pending.error);
	if (!pending.header) {
		throw DataCorruptionException("Recluster WAL transaction has no header");
	}
	try {
		pending.header->Validate();
	} catch (Exception &ex) {
		throw DataCorruptionException("Invalid recluster WAL header: %s", ex.what());
	}
	if (pending.deletes.size() != pending.header->delete_chunk_count) {
		throw DataCorruptionException("Recluster WAL DELETE chunk count does not match its header");
	}

	vector<row_t> result;
	if (pending.header->final_delete_row_count > NumericLimits<idx_t>::Maximum()) {
		throw DataCorruptionException("Recluster WAL DELETE row count exceeds the addressable range");
	}
	result.reserve(NumericCast<idx_t>(pending.header->final_delete_row_count));
	for (idx_t chunk_index = 0; chunk_index < pending.deletes.size(); chunk_index++) {
		auto &chunk = pending.deletes[chunk_index];
		try {
			chunk.Validate();
		} catch (Exception &ex) {
			throw DataCorruptionException("Invalid recluster WAL DELETE chunk: %s", ex.what());
		}
		if (chunk.table_id != pending.header->table_id || chunk.task_id != pending.header->task_id ||
		    chunk.chunk_index != chunk_index || chunk.new_rowids.size() > result.max_size() - result.size()) {
			throw DataCorruptionException("Recluster WAL DELETE chunks do not form a contiguous transaction");
		}
		result.insert(result.end(), chunk.new_rowids.begin(), chunk.new_rowids.end());
	}
	if (result.size() != pending.header->final_delete_row_count) {
		throw DataCorruptionException("Recluster WAL DELETE row count does not match its header");
	}
	for (idx_t row_index = 1; row_index < result.size(); row_index++) {
		if (result[row_index - 1] >= result[row_index]) {
			throw DataCorruptionException("Recluster WAL DELETE row IDs are not strictly increasing");
		}
	}
	return result;
}

static void ValidateManifest(const WALReclusterEntry &header, const ReplacementManifest &manifest,
                             const vector<row_t> &final_deleted_new_rowids, const vector<block_id_t> &manifest_blocks) {
	const char *mismatch = nullptr;
	if (manifest.payload_size != header.manifest_size) {
		throw DataCorruptionException(
		    "Recluster WAL header does not match its replacement manifest: payload size %llu does not match %llu at "
		    "block %lld (header table %s task %s, disk table %s task %s revision %llu)",
		    header.manifest_size, manifest.payload_size, header.manifest_pointer.GetBlockId(),
		    UUID::ToString(header.table_id), UUID::ToString(header.task_id), UUID::ToString(manifest.header.table_id),
		    UUID::ToString(manifest.header.task_id), manifest.header.manifest_revision);
	} else if (manifest.checksum != header.manifest_checksum) {
		throw DataCorruptionException(
		    "Recluster WAL header does not match its replacement manifest: checksum at block %lld (header table %s "
		    "task %s, disk table %s task %s revision %llu)",
		    header.manifest_pointer.GetBlockId(), UUID::ToString(header.table_id), UUID::ToString(header.task_id),
		    UUID::ToString(manifest.header.table_id), UUID::ToString(manifest.header.task_id),
		    manifest.header.manifest_revision);
	} else if (manifest.header.table_id != header.table_id) {
		mismatch = "table ID";
	} else if (manifest.header.task_id != header.task_id) {
		mismatch = "task ID";
	} else if (manifest.header.input_range.start != header.range_start ||
	           manifest.header.input_range.end != header.range_end) {
		mismatch = "input range";
	} else if (header.journal_resolved_through < manifest.header.last_applied_delete_sequence) {
		mismatch = "DELETE journal sequence";
	}
	if (mismatch) {
		throw DataCorruptionException("Recluster WAL header does not match its replacement manifest: %s", mismatch);
	}
	for (auto block_id : manifest.all_referenced_blocks) {
		if (std::find(manifest_blocks.begin(), manifest_blocks.end(), block_id) != manifest_blocks.end()) {
			throw DataCorruptionException("Recluster manifest chain overlaps its replacement block set");
		}
	}

	auto replacement_end = header.range_start;
	for (auto &row_group : manifest.replacement_groups) {
		replacement_end = NumericCast<row_t>(row_group.row_start + row_group.tuple_count);
	}
	for (auto row_id : final_deleted_new_rowids) {
		if (row_id < header.range_start || row_id >= replacement_end) {
			throw DataCorruptionException("Recluster WAL DELETE row ID is outside the replacement tuples");
		}
	}
}

static bool WALHeadersEqual(const WALReclusterEntry &left, const WALReclusterEntry &right) {
	return left.table_id == right.table_id && left.task_id == right.task_id &&
	       left.expected_layout_version == right.expected_layout_version &&
	       left.target_layout_version == right.target_layout_version && left.range_start == right.range_start &&
	       left.range_end == right.range_end && left.manifest_pointer == right.manifest_pointer &&
	       left.manifest_size == right.manifest_size && left.manifest_checksum == right.manifest_checksum &&
	       left.journal_resolved_through == right.journal_resolved_through &&
	       left.final_delete_row_count == right.final_delete_row_count &&
	       left.delete_chunk_count == right.delete_chunk_count;
}

static bool CheckPhysicalColumns(const DataTable &storage, const ReplacementManifest &manifest) {
	if (storage.Columns().size() != manifest.physical_columns.size()) {
		return false;
	}
	for (idx_t column_index = 0; column_index < storage.Columns().size(); column_index++) {
		auto &column = storage.Columns()[column_index];
		auto &manifest_column = manifest.physical_columns[column_index];
		if (column.PersistentColumnId() != manifest_column.column_id || column.Type() != manifest_column.type) {
			return false;
		}
	}
	return true;
}

static bool CheckOldRowGroups(RowGroupCollection &collection, const vector<ColumnDefinition> &columns,
                              const ReplacementManifest &manifest) {
	auto snapshot = collection.GetCurrentSnapshot();
	if (snapshot.kind != RowGroupCollectionSnapshot::Kind::VERSIONED_LAYOUT) {
		return false;
	}
	for (auto &patch : snapshot.layout->patches) {
		if (patch->range.Overlaps(manifest.header.input_range)) {
			return false;
		}
	}
	for (auto &expected : manifest.old_groups) {
		LayoutRowGroupEntry current;
		if (!snapshot.Lookup(expected.start, current) || current.row_start != expected.start ||
		    current.GetRowEnd() != expected.start + NumericCast<row_t>(expected.count)) {
			return false;
		}
		auto identity = ComputeRowGroupPhysicalIdentityV1(*current.row_group, current.row_start, columns);
		if (!identity || !(*identity == expected)) {
			return false;
		}
	}
	return true;
}

static void RegisterReplacementMetadata(BlockManager &block_manager, const ReplacementManifest &manifest) {
	auto &metadata_manager = block_manager.GetMetadataManager();
	auto register_pointer = [&](MetaBlockPointer pointer) {
		if (pointer.IsValid()) {
			metadata_manager.RegisterRecoveryDiskPointer(pointer);
		}
	};
	for (auto &row_group : manifest.replacement_groups) {
		for (auto pointer : row_group.data_pointers) {
			register_pointer(pointer);
		}
		for (auto pointer : row_group.deletes_pointers) {
			register_pointer(pointer);
		}
		for (auto block_id : row_group.extra_metadata_blocks) {
			register_pointer(MetaBlockPointer(block_id, 0));
		}
		row_group.per_column_metadata_blocks.ForEachBlock(
		    [&](idx_t, idx_t block_id) { register_pointer(MetaBlockPointer(block_id, 0)); });
	}
}

static idx_t ApplyFinalDeletes(const ReplacementManifest &manifest, const vector<shared_ptr<RowGroup>> &row_groups,
                               const vector<row_t> &row_ids) {
	idx_t deleted_count = 0;
	idx_t row_index = 0;
	idx_t group_index = 0;
	while (row_index < row_ids.size()) {
		while (group_index < manifest.replacement_groups.size() &&
		       row_ids[row_index] >= NumericCast<row_t>(manifest.replacement_groups[group_index].row_start +
		                                                manifest.replacement_groups[group_index].tuple_count)) {
			group_index++;
		}
		if (group_index >= row_groups.size()) {
			throw DataCorruptionException("Recluster WAL DELETE row ID has no replacement row group");
		}
		auto group_start = NumericCast<row_t>(manifest.replacement_groups[group_index].row_start);
		auto group_end = NumericCast<row_t>(manifest.replacement_groups[group_index].row_start +
		                                    manifest.replacement_groups[group_index].tuple_count);
		if (row_ids[row_index] < group_start) {
			throw DataCorruptionException("Recluster WAL DELETE row IDs are not contiguous with replacement groups");
		}

		auto vector_index = NumericCast<idx_t>((row_ids[row_index] - group_start) / STANDARD_VECTOR_SIZE);
		auto vector_start = group_start + NumericCast<row_t>(vector_index * STANDARD_VECTOR_SIZE);
		auto vector_end = MinValue<row_t>(group_end, vector_start + NumericCast<row_t>(STANDARD_VECTOR_SIZE));
		row_t offsets[STANDARD_VECTOR_SIZE];
		idx_t offset_count = 0;
		while (row_index < row_ids.size() && row_ids[row_index] < vector_end) {
			offsets[offset_count++] = row_ids[row_index++] - vector_start;
		}
		deleted_count +=
		    row_groups[group_index]->GetOrCreateVersionInfo().DeleteCommittedRows(vector_index, offsets, offset_count);
	}
	return deleted_count;
}

class ReclusterWALReplayContextState {
public:
	explicit ReclusterWALReplayContextState(AttachedDatabase &db_p) : db(db_p) {
	}

	void RebuildTableMap() {
		tables.clear();
		auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
		catalog.ScanSchemas([&](SchemaCatalogEntry &schema) {
			schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.internal || entry.type != CatalogType::TABLE_ENTRY) {
					return;
				}
				auto &table = entry.Cast<DuckTableEntry>();
				if (!table.HasSortHistory()) {
					return;
				}
				auto table_id = table.GetSortMetadata()->table_id;
				if (!tables.emplace(table_id, optional_ptr<DuckTableEntry>(table)).second) {
					throw DataCorruptionException("Duplicate persistent SORTED BY table ID during WAL recovery");
				}
			});
		});
	}

	idx_t ReserveHeader(const WALReclusterEntry &header) {
		records.reserve(records.size() + 1);
		auto &block_manager = db.GetStorageManager().GetBlockManager();
		ReclusterWALReplayRecord record;
		record.header = header;
		record.manifest_blocks.reserve(4);
		auto temporary_reservation = AllocatorBlockReservation::Reserve(block_manager, {});
		idx_t marked_blocks = 0;
		try {
			header.Validate();
			ProtectedManifestReader reader(block_manager, header.manifest_pointer, record.manifest_blocks,
			                               temporary_reservation);
			record.manifest = ReplacementManifest::Read(reader);
			record.replacement_blocks = record.manifest.all_referenced_blocks;
			for (auto block_id : record.replacement_blocks) {
				if (!block_manager.Cast<SingleFileBlockManager>().BlockExistsOnDisk(block_id)) {
					throw DataCorruptionException("Recluster manifest references block %d outside the database file",
					                              block_id);
				}
				temporary_reservation.AddPhysicalBlock(block_id);
				marked_blocks++;
			}
			ValidateManifest(header, record.manifest, {}, record.manifest_blocks);
		} catch (Exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::OUT_OF_MEMORY || error.Type() == ExceptionType::INTERRUPT ||
			    error.Type() == ExceptionType::FATAL || error.Type() == ExceptionType::INTERNAL) {
				throw;
			}
			record.replacement_blocks.resize(marked_blocks);
			record.validation_error = error.RawMessage();
		}
		if (!record.manifest_blocks.empty() || !record.replacement_blocks.empty()) {
			record.wal_reservation = db.GetReclusterManager().GetWALBlockRetention().Reserve(record.manifest_blocks,
			                                                                                 record.replacement_blocks);
		}
		records.push_back(std::move(record));
		return records.size() - 1;
	}

	void CommitReservation(idx_t record_index, vector<row_t> final_deleted_new_rowids,
	                       ReclusterWALPosition transaction_end) {
		if (record_index >= records.size()) {
			throw InternalException("Invalid recluster WAL reservation index");
		}
		auto &record = records[record_index];
		if (record.validation_error) {
			throw DataCorruptionException("Invalid recluster replacement manifest: %s", *record.validation_error);
		}
		if (record.committed || !WALHeadersEqual(record.header, *pending.header)) {
			throw DataCorruptionException("Recluster WAL reservation does not match its transaction");
		}
		ValidateManifest(record.header, record.manifest, final_deleted_new_rowids, record.manifest_blocks);
		if (!record.wal_reservation.IsActive() || transaction_end.file_offset == 0) {
			throw InternalException("Invalid recluster WAL retention reservation");
		}
		committed_record_indexes.reserve(committed_record_indexes.size() + 1);
		record.final_deleted_new_rowids = std::move(final_deleted_new_rowids);
		record.transaction_end = transaction_end;
		record.committed = true;
		committed_record_indexes.push_back(record_index);
	}

	void PrepareReplayRecord(ClientContext &context, vector<row_t> final_deleted_new_rowids,
	                         ReclusterWALPosition transaction_end) {
		if (next_record >= committed_record_indexes.size()) {
			throw DataCorruptionException("Recluster WAL replay contains an unreserved transaction");
		}
		if (pending_replay_commit_record.IsValid()) {
			throw InternalException("Previous recluster WAL replay transaction was not committed");
		}
		auto record_index = committed_record_indexes[next_record++];
		auto &record = records[record_index];
		if (!record.committed || !WALHeadersEqual(record.header, *pending.header) ||
		    record.final_deleted_new_rowids != final_deleted_new_rowids ||
		    record.transaction_end.checkpoint_iteration != transaction_end.checkpoint_iteration ||
		    record.transaction_end.file_offset != transaction_end.file_offset || !record.wal_reservation.IsActive()) {
			throw DataCorruptionException("Recluster WAL changed between recovery pre-scan and replay");
		}
		pending_replay_commit_record = record_index;

		auto table_entry = tables.find(record.header.table_id);
		if (table_entry == tables.end()) {
			throw DataCorruptionException("Recluster WAL references an unknown persistent table ID");
		}
		auto &table = *table_entry->second;
		auto storage = table.GetStorage().shared_from_this();
		if (!storage->GetDataTableInfo()->HasSortStorage()) {
			throw DataCorruptionException("Recluster WAL references a table without SORTED BY storage state");
		}
		auto &sort_storage = storage->GetDataTableInfo()->GetSortStorage();
		auto current_version = sort_storage.current_layout_version.load();
		if (current_version >= record.header.target_layout_version) {
			if (sort_storage.next_run_id.load() <= record.manifest.header.run_id) {
				throw DataCorruptionException("Checkpointed SORTED BY run ID is behind skipped recluster WAL");
			}
			return;
		}
		if (current_version != record.header.expected_layout_version) {
			throw DataCorruptionException("Recluster WAL layout version chain is not contiguous");
		}

		auto definition = table.GetSortMetadata() ? table.GetSortMetadata()->GetCurrent() : nullptr;
		auto collection = storage->GetRowGroupCollection();
		auto old_layout = collection->GetCurrentLayout();
		if (!storage->IsMainTable() || !table.SortEnabled() || !definition ||
		    definition->sort_order_id != record.manifest.header.sort_order_id ||
		    definition->columns != record.manifest.sort_columns ||
		    table.GetSortMetadata()->table_id != record.header.table_id || !old_layout ||
		    old_layout->layout_version != record.header.expected_layout_version ||
		    old_layout->patches.size() >= MAX_LAYOUT_PATCHES_PER_CHECKPOINT ||
		    !CheckPhysicalColumns(*storage, record.manifest) ||
		    !CheckOldRowGroups(*collection, storage->Columns(), record.manifest)) {
			throw DataCorruptionException("Recluster WAL does not match the recovered table layout");
		}

		auto &block_manager = db.GetStorageManager().GetBlockManager();
		RegisterReplacementMetadata(block_manager, record.manifest);
		vector<shared_ptr<RowGroup>> replacement_groups;
		replacement_groups.reserve(record.manifest.replacement_groups.size());
		for (auto &pointer : record.manifest.replacement_groups) {
			replacement_groups.push_back(make_shared_ptr<RowGroup>(*collection, pointer));
		}
		if (ApplyFinalDeletes(record.manifest, replacement_groups, record.final_deleted_new_rowids) !=
		    record.final_deleted_new_rowids.size()) {
			throw DataCorruptionException("Recluster WAL DELETE repeats a delete already stored in its manifest");
		}

		idx_t replaced_rows = 0;
		for (auto &old_group : record.manifest.old_groups) {
			replaced_rows += old_group.count;
		}
		idx_t replacement_rows = 0;
		for (auto &replacement : record.manifest.replacement_groups) {
			replacement_rows += replacement.tuple_count;
		}
		auto patch = make_shared_ptr<LayoutPatch>();
		patch->task_id = record.header.task_id;
		patch->range = {record.header.range_start, record.header.range_end};
		patch->sort_order_id = record.manifest.header.sort_order_id;
		patch->run_id = record.manifest.header.run_id;
		patch->replaced_physical_rows = replaced_rows;
		patch->replacement_physical_rows = replacement_rows;
		patch->replacement_groups = std::move(replacement_groups);
		auto pending_layout = collection->BuildPendingPatchedLayout(std::move(patch));
		auto commit_info = make_uniq<ReclusterCommitInfo>(
		    storage, old_layout, std::move(pending_layout), record.manifest.header.run_id,
		    std::move(record.replacement_blocks), RowGroupRange {record.header.range_start, record.header.range_end});
		auto &transaction = DuckTransaction::Get(context, db);
		transaction.SetIsReclusterMaintenanceTransaction();
		transaction.PushRecluster(std::move(commit_info));
	}

	void CommitPendingRetention() noexcept {
		if (!pending_replay_commit_record.IsValid()) {
			return;
		}
		auto &record = records[pending_replay_commit_record.GetIndex()];
		db.GetReclusterManager().GetWALBlockRetention().Commit(std::move(record.wal_reservation),
		                                                       record.transaction_end);
		pending_replay_commit_record = optional_idx();
	}

	void ReleaseAllReservations() noexcept {
		records.clear();
		committed_record_indexes.clear();
		next_record = 0;
		pending_replay_commit_record = optional_idx();
	}

	void ReleaseUncommittedReservations() noexcept {
		for (auto &record : records) {
			if (!record.committed) {
				record.wal_reservation = ReclusterWALRetentionReservation();
			}
		}
	}

	void RetainUncommittedReservationsUntilShutdown() noexcept {
		auto &retention = db.GetReclusterManager().GetWALBlockRetention();
		for (auto &record : records) {
			if (!record.committed && record.wal_reservation.IsActive()) {
				retention.RetainUntilShutdown(std::move(record.wal_reservation));
			}
		}
	}

	bool HasUncommittedReclusterTransaction() const {
		if (pending.HasReclusterEntries()) {
			return true;
		}
		for (auto &record : records) {
			if (!record.committed) {
				return true;
			}
		}
		return false;
	}

public:
	AttachedDatabase &db;
	ReclusterWALReplayPhase phase = ReclusterWALReplayPhase::PRESCAN;
	PendingReclusterWALTransaction pending;
	vector<ReclusterWALReplayRecord> records;
	vector<idx_t> committed_record_indexes;
	idx_t next_record = 0;
	optional_idx wal_checkpoint_iteration;
	optional_idx pending_replay_commit_record;
	bool rebuild_table_map_after_commit = false;
	unordered_map<persistent_table_id_t, optional_ptr<DuckTableEntry>> tables;
};

ReclusterWALReplayContext::ReclusterWALReplayContext(AttachedDatabase &db)
    : state(make_uniq<ReclusterWALReplayContextState>(db)) {
}

ReclusterWALReplayContext::~ReclusterWALReplayContext() {
}

void ReclusterWALReplayContext::SetWALCheckpointIteration(uint64_t checkpoint_iteration) {
	if (state->wal_checkpoint_iteration.IsValid() &&
	    state->wal_checkpoint_iteration.GetIndex() != checkpoint_iteration) {
		throw DataCorruptionException("Recluster WAL checkpoint iteration changed between recovery passes");
	}
	state->wal_checkpoint_iteration = checkpoint_iteration;
}

void ReclusterWALReplayContext::ObserveEntry(WALType type) {
	auto &pending = state->pending;
	pending.catalog_changed = pending.catalog_changed || WALTypeChangesCatalog(type);
	switch (type) {
	case WALType::RECLUSTER:
		if (pending.has_other_write) {
			pending.error = ReclusterWALTransactionError::MIXED_TRANSACTION;
		}
		break;
	case WALType::RECLUSTER_DELETE:
		break;
	case WALType::USE_TABLE:
		break;
	case WALType::WAL_VERSION:
		if (pending.HasReclusterEntries()) {
			pending.error = ReclusterWALTransactionError::MIXED_TRANSACTION;
		}
		break;
	default:
		if (pending.HasReclusterEntries()) {
			pending.error = ReclusterWALTransactionError::MIXED_TRANSACTION;
		}
		pending.has_other_write = true;
		break;
	}
}

void ReclusterWALReplayContext::AddHeader(WALReclusterEntry entry) {
	auto &pending = state->pending;
	if (state->phase == ReclusterWALReplayPhase::PRESCAN) {
		pending.reservation_indexes.push_back(state->ReserveHeader(entry));
	}
	if (pending.header) {
		pending.error = ReclusterWALTransactionError::MULTIPLE_HEADERS;
	} else {
		pending.header = std::move(entry);
	}
	if (pending.has_other_write) {
		pending.error = ReclusterWALTransactionError::MIXED_TRANSACTION;
	}
}

void ReclusterWALReplayContext::AddDelete(WALReclusterDeleteEntry entry) {
	auto &pending = state->pending;
	if (!pending.header) {
		pending.error = ReclusterWALTransactionError::DELETE_WITHOUT_HEADER;
	}
	pending.deletes.push_back(std::move(entry));
}

void ReclusterWALReplayContext::FinishTransaction(optional_idx transaction_end, optional_ptr<ClientContext> context) {
	auto &pending = state->pending;
	if (!pending.HasReclusterEntries()) {
		if (state->phase == ReclusterWALReplayPhase::REPLAY) {
			state->rebuild_table_map_after_commit = pending.catalog_changed;
		}
		pending.Reset();
		return;
	}
	if (!state->wal_checkpoint_iteration.IsValid() || !transaction_end.IsValid() || transaction_end.GetIndex() == 0) {
		throw DataCorruptionException("Recluster WAL transaction is missing its physical WAL position");
	}
	ReclusterWALPosition position {state->wal_checkpoint_iteration.GetIndex(), transaction_end.GetIndex()};
	auto final_deleted_new_rowids = ValidateWALTransaction(pending);
	if (state->phase == ReclusterWALReplayPhase::PRESCAN) {
		if (pending.reservation_indexes.size() != 1) {
			throw InternalException("Recluster WAL transaction has an invalid reservation count");
		}
		state->CommitReservation(pending.reservation_indexes[0], std::move(final_deleted_new_rowids), position);
	} else {
		if (!context) {
			throw InternalException("Recluster WAL replay requires a client context");
		}
		state->PrepareReplayRecord(*context, std::move(final_deleted_new_rowids), position);
		state->rebuild_table_map_after_commit = pending.catalog_changed;
	}
	pending.Reset();
}

void ReclusterWALReplayContext::BeginReplay() {
	state->pending.Reset();
	state->phase = ReclusterWALReplayPhase::REPLAY;
	state->next_record = 0;
	state->RebuildTableMap();
}

void ReclusterWALReplayContext::OnTransactionCommitted() {
	if (state->phase != ReclusterWALReplayPhase::REPLAY) {
		throw InternalException("Cannot update the recluster table map before WAL replay");
	}
	state->CommitPendingRetention();
	if (state->rebuild_table_map_after_commit) {
		state->RebuildTableMap();
		state->rebuild_table_map_after_commit = false;
	}
}

void ReclusterWALReplayContext::VerifyReplayComplete(bool all_succeeded) const {
	if (state->phase != ReclusterWALReplayPhase::REPLAY || (all_succeeded && state->pending.HasReclusterEntries()) ||
	    state->next_record != state->committed_record_indexes.size() || state->pending_replay_commit_record.IsValid()) {
		throw DataCorruptionException("Recluster WAL recovery did not consume every reserved transaction");
	}
}

bool ReclusterWALReplayContext::HasUncommittedReclusterTransaction() const {
	return state->HasUncommittedReclusterTransaction();
}

void ReclusterWALReplayContext::ReleaseUncommittedReservations() noexcept {
	state->ReleaseUncommittedReservations();
}

void ReclusterWALReplayContext::RetainUncommittedReservationsUntilShutdown() noexcept {
	state->RetainUncommittedReservationsUntilShutdown();
}

void ReclusterWALReplayContext::ReleaseAllReservations() noexcept {
	state->ReleaseAllReservations();
}

} // namespace duckdb
