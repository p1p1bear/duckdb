//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class DatabaseInstance;
class MetadataManager;
struct MetadataBlockInfo;

struct MetadataBlock {
	MetadataBlock();
	// disable copy constructors
	MetadataBlock(const MetadataBlock &other) = delete;
	MetadataBlock &operator=(const MetadataBlock &) = delete;
	//! enable move constructors
	DUCKDB_API MetadataBlock(MetadataBlock &&other) noexcept;
	DUCKDB_API MetadataBlock &operator=(MetadataBlock &&) noexcept;

	shared_ptr<BlockHandle> block;
	block_id_t block_id;
	vector<uint8_t> free_blocks;
	atomic<bool> dirty;

	void Write(WriteStream &sink);
	static MetadataBlock Read(ReadStream &source);

	idx_t FreeBlocksToInteger();
	void FreeBlocksFromInteger(idx_t blocks);
	static vector<uint8_t> BlocksFromInteger(idx_t free_list);

	string ToString() const;
};

struct MetadataPointer {
	idx_t block_index : 56;
	uint8_t index : 8;
};

struct MetadataHandle {
	MetadataPointer pointer;
	BufferHandle handle;
};

class TaskPrivateMetadataBlockOwner {
public:
	~TaskPrivateMetadataBlockOwner();

	TaskPrivateMetadataBlockOwner(const TaskPrivateMetadataBlockOwner &) = delete;
	TaskPrivateMetadataBlockOwner &operator=(const TaskPrivateMetadataBlockOwner &) = delete;

	MetadataHandle AllocateHandle();
	void Flush(QueryContext context = QueryContext());
	vector<block_id_t> GetBlockIds() const;
	void MarkPublished();
	void Abort();

	MetadataManager &GetManager() {
		return manager;
	}

private:
	friend class MetadataManager;

	TaskPrivateMetadataBlockOwner(MetadataManager &manager, uint64_t owner_id);

private:
	MetadataManager &manager;
	uint64_t owner_id;
	bool active = true;
};

class MetadataManager {
public:
	//! The amount of metadata blocks per storage block
	static constexpr const idx_t METADATA_BLOCK_COUNT = 64;

public:
	MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager);
	~MetadataManager();

	BufferManager &GetBufferManager() const {
		return buffer_manager;
	}

	MetadataHandle AllocateHandle();
	unique_ptr<TaskPrivateMetadataBlockOwner> CreateTaskPrivateBlockOwner();
	MetadataHandle Pin(const MetadataPointer &pointer);

	MetadataHandle Pin(const QueryContext &context, const MetadataPointer &pointer);

	MetaBlockPointer GetDiskPointer(const MetadataPointer &pointer, uint32_t offset = 0);
	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);
	MetadataPointer RegisterDiskPointer(MetaBlockPointer pointer);
	//! Register metadata introduced after the checkpoint and include it in the next preserve-or-free pass.
	MetadataPointer RegisterRecoveryDiskPointer(MetaBlockPointer pointer);

	static BlockPointer ToBlockPointer(MetaBlockPointer meta_pointer, const idx_t metadata_block_size);
	static MetaBlockPointer FromBlockPointer(BlockPointer block_pointer, const idx_t metadata_block_size);

	//! Flush all blocks to disk
	void Flush(QueryContext context = QueryContext());

	bool BlockIsModified(const MetaBlockPointer &ptr);
	bool BlockHasBeenCleared(const MetaBlockPointer &ptr);

	void MarkBlocksAsModified();
	void ClearModifiedBlocks(const vector<MetaBlockPointer> &pointers);

	vector<MetadataBlockInfo> GetMetadataInfo() const;
	vector<shared_ptr<BlockHandle>> GetBlocks() const;
	idx_t BlockCount();

	void Write(WriteStream &sink);
	void Read(ReadStream &source);

	idx_t GetMetadataBlockSize() const;

protected:
	BlockManager &block_manager;
	BufferManager &buffer_manager;
	mutable mutex block_lock;
	unordered_map<block_id_t, MetadataBlock> blocks;
	unordered_map<block_id_t, idx_t> modified_blocks;
	unordered_map<block_id_t, uint64_t> task_private_blocks;
	unordered_map<uint64_t, vector<block_id_t>> task_private_owners;
	uint64_t next_task_private_owner_id = 1;

protected:
	block_id_t AllocateNewBlock(unique_lock<mutex> &block_lock);
	block_id_t AllocateNewTaskPrivateBlock(unique_lock<mutex> &block_lock, uint64_t owner_id);
	block_id_t PeekNextBlockId() const;
	block_id_t GetNextBlockId() const;

	void AddBlock(unique_lock<mutex> &block_lock, MetadataBlock new_block, bool if_exists = false);
	void AddAndRegisterBlock(unique_lock<mutex> &block_lock, MetadataBlock block);
	void ConvertToTransient(unique_lock<mutex> &block_lock, block_id_t block_id);
	MetadataPointer FromDiskPointerInternal(unique_lock<mutex> &block_lock, MetaBlockPointer pointer);
	void FlushBlock(block_id_t block_id, QueryContext context);

private:
	friend class TaskPrivateMetadataBlockOwner;

	uint64_t RegisterTaskPrivateOwner();
	MetadataHandle AllocateTaskPrivateHandle(uint64_t owner_id);
	void FlushTaskPrivateBlocks(uint64_t owner_id, QueryContext context);
	vector<block_id_t> GetTaskPrivateBlockIds(uint64_t owner_id) const;
	void PublishTaskPrivateBlocks(uint64_t owner_id);
	void AbortTaskPrivateBlocks(uint64_t owner_id);
};

} // namespace duckdb
