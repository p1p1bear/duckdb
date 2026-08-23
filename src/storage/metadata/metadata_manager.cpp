#include "duckdb/storage/metadata/metadata_manager.hpp"

#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

MetadataBlock::MetadataBlock() : block_id(INVALID_BLOCK), dirty(false) {
}

MetadataBlock::MetadataBlock(MetadataBlock &&other) noexcept : dirty(false) {
	std::swap(block, other.block);
	std::swap(block_id, other.block_id);
	std::swap(free_blocks, other.free_blocks);
	auto dirty_val = dirty.load();
	dirty = other.dirty.load();
	other.dirty = dirty_val;
}

MetadataBlock &MetadataBlock::operator=(MetadataBlock &&other) noexcept {
	std::swap(block, other.block);
	std::swap(block_id, other.block_id);
	std::swap(free_blocks, other.free_blocks);
	auto dirty_val = dirty.load();
	dirty = other.dirty.load();
	other.dirty = dirty_val;
	return *this;
}

string MetadataBlock::ToString() const {
	string result;
	for (idx_t i = 0; i < MetadataManager::METADATA_BLOCK_COUNT; i++) {
		if (std::find(free_blocks.begin(), free_blocks.end(), i) != free_blocks.end()) {
			if (!result.empty()) {
				result += ", ";
			}
			result += to_string(i);
		}
	}
	return "block_id: " + to_string(block_id) + " [" + result + "]";
}

MetadataManager::MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager)
    : block_manager(block_manager), buffer_manager(buffer_manager) {
}

MetadataManager::~MetadataManager() {
}

TaskPrivateMetadataBlockOwner::TaskPrivateMetadataBlockOwner(MetadataManager &manager_p, uint64_t owner_id_p)
    : manager(manager_p), owner_id(owner_id_p) {
}

TaskPrivateMetadataBlockOwner::~TaskPrivateMetadataBlockOwner() {
	if (!active) {
		return;
	}
	try {
		Abort();
	} catch (...) { // NOLINT: destructors cannot report asynchronous cleanup failures
	}
}

MetadataHandle TaskPrivateMetadataBlockOwner::AllocateHandle() {
	if (!active) {
		throw InternalException("Cannot allocate from an inactive task-private metadata owner");
	}
	return manager.AllocateTaskPrivateHandle(owner_id);
}

void TaskPrivateMetadataBlockOwner::Flush(QueryContext context) {
	if (!active) {
		throw InternalException("Cannot flush an inactive task-private metadata owner");
	}
	manager.FlushTaskPrivateBlocks(owner_id, context);
}

vector<block_id_t> TaskPrivateMetadataBlockOwner::GetBlockIds() const {
	if (!active) {
		throw InternalException("Cannot inspect an inactive task-private metadata owner");
	}
	return manager.GetTaskPrivateBlockIds(owner_id);
}

void TaskPrivateMetadataBlockOwner::MarkPublished() {
	if (!active) {
		throw InternalException("Cannot publish an inactive task-private metadata owner");
	}
	manager.PublishTaskPrivateBlocks(owner_id);
	active = false;
}

void TaskPrivateMetadataBlockOwner::Abort() {
	if (!active) {
		return;
	}
	manager.AbortTaskPrivateBlocks(owner_id);
	active = false;
}

unique_ptr<TaskPrivateMetadataBlockOwner> MetadataManager::CreateTaskPrivateBlockOwner() {
	return unique_ptr<TaskPrivateMetadataBlockOwner>(
	    new TaskPrivateMetadataBlockOwner(*this, RegisterTaskPrivateOwner()));
}

uint64_t MetadataManager::RegisterTaskPrivateOwner() {
	lock_guard<mutex> guard(block_lock);
	if (next_task_private_owner_id == 0) {
		throw InternalException("Task-private metadata owner ID space is exhausted");
	}
	auto owner_id = next_task_private_owner_id++;
	if (!task_private_owners.emplace(owner_id, vector<block_id_t>()).second) {
		throw InternalException("Task-private metadata owner ID is already registered");
	}
	return owner_id;
}

MetadataHandle MetadataManager::AllocateHandle() {
	// check if there is any free space left in an existing block
	// if not allocate a new block
	MetadataPointer pointer;
	unique_lock<mutex> guard(block_lock);
	block_id_t free_block = INVALID_BLOCK;
	for (auto &kv : blocks) {
		auto &block = kv.second;
		D_ASSERT(kv.first == block.block_id);
		if (task_private_blocks.find(kv.first) == task_private_blocks.end() && !block.free_blocks.empty()) {
			free_block = kv.first;
			break;
		}
	}
	guard.unlock();
	if (free_block == INVALID_BLOCK || free_block > PeekNextBlockId()) {
		free_block = AllocateNewBlock(guard);
	} else {
		guard.lock();
	}
	D_ASSERT(guard.owns_lock());
	D_ASSERT(free_block != INVALID_BLOCK);

	// select the first free metadata block we can find
	pointer.block_index = UnsafeNumericCast<idx_t>(free_block);
	auto &block = blocks.at(free_block);
	// the block is now dirty
	block.dirty = true;
	if (block.block->BlockId() < MAXIMUM_BLOCK) {
		// this block is a disk-backed block, yet we are planning to write to it
		// we need to convert it into a transient block before we can write to it
		ConvertToTransient(guard, free_block);
	}
	auto &writable_block = blocks.at(free_block);
	D_ASSERT(writable_block.block->BlockId() >= MAXIMUM_BLOCK);
	D_ASSERT(!writable_block.free_blocks.empty());
	pointer.index = writable_block.free_blocks.back();
	// mark the block as used
	writable_block.free_blocks.pop_back();
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	guard.unlock();
	// pin the block
	return Pin(pointer);
}

MetadataHandle MetadataManager::AllocateTaskPrivateHandle(uint64_t owner_id) {
	MetadataPointer pointer;
	unique_lock<mutex> guard(block_lock);
	auto owner_entry = task_private_owners.find(owner_id);
	if (owner_entry == task_private_owners.end()) {
		throw InternalException("Task-private metadata owner does not exist");
	}

	block_id_t free_block = INVALID_BLOCK;
	for (auto block_id : owner_entry->second) {
		auto block_entry = blocks.find(block_id);
		auto private_entry = task_private_blocks.find(block_id);
		if (block_entry == blocks.end() || private_entry == task_private_blocks.end() ||
		    private_entry->second != owner_id) {
			throw InternalException("Task-private metadata block ownership is inconsistent");
		}
		if (!block_entry->second.free_blocks.empty()) {
			free_block = block_id;
			break;
		}
	}
	if (free_block == INVALID_BLOCK) {
		guard.unlock();
		free_block = AllocateNewTaskPrivateBlock(guard, owner_id);
	}
	D_ASSERT(guard.owns_lock());

	auto &block = blocks.at(free_block);
	block.dirty = true;
	if (block.block->BlockId() < MAXIMUM_BLOCK) {
		ConvertToTransient(guard, free_block);
	}
	auto &writable_block = blocks.at(free_block);
	D_ASSERT(writable_block.block->BlockId() >= MAXIMUM_BLOCK);
	D_ASSERT(!writable_block.free_blocks.empty());
	pointer.block_index = UnsafeNumericCast<idx_t>(free_block);
	pointer.index = writable_block.free_blocks.back();
	writable_block.free_blocks.pop_back();
	guard.unlock();
	return Pin(pointer);
}

MetadataHandle MetadataManager::Pin(const MetadataPointer &pointer) {
	return Pin(QueryContext(), pointer);
}

MetadataHandle MetadataManager::Pin(const QueryContext &context, const MetadataPointer &pointer) {
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	shared_ptr<BlockHandle> block_handle;
	{
		lock_guard<mutex> guard(block_lock);
		auto entry = blocks.find(UnsafeNumericCast<int64_t>(pointer.block_index));
		if (entry == blocks.end()) {
			throw InternalException("Trying to pin block %llu - but the block did not exist", pointer.block_index);
		}
		auto &block = entry->second;
#ifdef DEBUG
		for (auto &free_block : block.free_blocks) {
			if (free_block == pointer.index) {
				throw InternalException("Pinning block %d.%d but it is marked as a free block", block.block_id,
				                        free_block);
			}
		}
#endif
		block_handle = block.block;
	}

	MetadataHandle handle;
	handle.pointer.block_index = pointer.block_index;
	handle.pointer.index = pointer.index;
	handle.handle = buffer_manager.Pin(block_handle);
	return handle;
}

void MetadataManager::ConvertToTransient(unique_lock<mutex> &block_lock, block_id_t block_id) {
	D_ASSERT(block_lock.owns_lock());
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		throw InternalException("Cannot convert a missing metadata block to transient storage");
	}
	auto old_block = entry->second.block;
	block_lock.unlock();
	// pin the old block
	auto old_buffer = buffer_manager.Pin(old_block);

	// allocate a new transient block to replace it
	auto new_buffer = buffer_manager.Allocate(MemoryTag::METADATA, &block_manager, false);
	auto new_block = new_buffer.GetBlockHandle();

	// copy the data to the transient block
	memcpy(new_buffer.GetDataMutable(), old_buffer.Ptr(), block_manager.GetBlockSize());

	// unregister the old block
	block_manager.UnregisterBlock(block_id);

	block_lock.lock();
	entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		throw InternalException("Metadata block disappeared while converting it to transient storage");
	}
	entry->second.block = std::move(new_block);
	entry->second.dirty = true;
}

block_id_t MetadataManager::AllocateNewBlock(unique_lock<mutex> &block_lock) {
	D_ASSERT(!block_lock.owns_lock());
	auto new_block_id = GetNextBlockId();

	MetadataBlock new_block;
	auto handle = buffer_manager.Allocate(MemoryTag::METADATA, &block_manager, false);
	new_block.block = handle.GetBlockHandle();
	new_block.block_id = new_block_id;
	for (idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(NumericCast<uint8_t>(METADATA_BLOCK_COUNT - i - 1));
	}
	new_block.dirty = true;
	// zero-initialize the handle
	memset(handle.GetDataMutable(), 0, block_manager.GetBlockSize());

	block_lock.lock();
	AddBlock(block_lock, std::move(new_block));
	return new_block_id;
}

block_id_t MetadataManager::AllocateNewTaskPrivateBlock(unique_lock<mutex> &block_lock, uint64_t owner_id) {
	D_ASSERT(!block_lock.owns_lock());
	auto handle = buffer_manager.Allocate(MemoryTag::METADATA, &block_manager, false);
	memset(handle.GetDataMutable(), 0, block_manager.GetBlockSize());
	auto new_block_id = block_manager.GetFreeBlockId();

	MetadataBlock new_block;
	new_block.block = handle.GetBlockHandle();
	new_block.block_id = new_block_id;
	for (idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(NumericCast<uint8_t>(METADATA_BLOCK_COUNT - i - 1));
	}
	new_block.dirty = true;

	block_lock.lock();
	auto owner_entry = task_private_owners.find(owner_id);
	if (owner_entry == task_private_owners.end()) {
		block_lock.unlock();
		new_block.block.reset();
		handle.Destroy();
		block_manager.MarkBlockAsModified(new_block_id);
		throw InternalException("Task-private metadata owner was removed during allocation");
	}
	AddBlock(block_lock, std::move(new_block));
	if (!task_private_blocks.emplace(new_block_id, owner_id).second) {
		throw InternalException("Task-private metadata block was already owned");
	}
	owner_entry->second.push_back(new_block_id);
	return new_block_id;
}

void MetadataManager::AddBlock(unique_lock<mutex> &block_lock, MetadataBlock new_block, bool if_exists) {
	D_ASSERT(block_lock.owns_lock());
	if (blocks.find(new_block.block_id) != blocks.end()) {
		if (if_exists) {
			return;
		}
		throw InternalException("Block id with id %llu already exists", new_block.block_id);
	}
	blocks[new_block.block_id] = std::move(new_block);
}

void MetadataManager::AddAndRegisterBlock(unique_lock<mutex> &block_lock, MetadataBlock block) {
	if (block.block) {
		throw InternalException("Calling AddAndRegisterBlock on block that already exists");
	}
	if (block.block_id >= MAXIMUM_BLOCK) {
		throw InternalException("AddAndRegisterBlock called with a transient block id");
	}
	block_lock.unlock();
	block.block = block_manager.RegisterBlock(block.block_id);
	block_lock.lock();
	AddBlock(block_lock, std::move(block), true);
}

MetaBlockPointer MetadataManager::GetDiskPointer(const MetadataPointer &pointer, uint32_t offset) {
	idx_t block_pointer = idx_t(pointer.block_index);
	block_pointer |= idx_t(pointer.index) << 56ULL;
	return MetaBlockPointer(block_pointer, offset);
}

block_id_t MetaBlockPointer::GetBlockId() const {
	return block_id_t(block_pointer & ~(idx_t(0xFF) << 56ULL));
}

uint32_t MetaBlockPointer::GetBlockIndex() const {
	return block_pointer >> 56ULL;
}

MetadataPointer MetadataManager::FromDiskPointer(MetaBlockPointer pointer) {
	unique_lock<mutex> guard(block_lock);
	return FromDiskPointerInternal(guard, pointer);
}

MetadataPointer MetadataManager::FromDiskPointerInternal(unique_lock<mutex> &block_lock, MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	auto index = pointer.GetBlockIndex();
	if (index >= METADATA_BLOCK_COUNT) {
		throw IOException("Metadata block index %llu exceeds metadata block count %llu", index, METADATA_BLOCK_COUNT);
	}

	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) { // LCOV_EXCL_START
		throw InternalException("Failed to load metadata pointer (id %llu, idx %llu, ptr %llu)\n", block_id, index,
		                        pointer.block_pointer);
	} // LCOV_EXCL_STOP
	MetadataPointer result;
	result.block_index = UnsafeNumericCast<idx_t>(block_id);
	result.index = UnsafeNumericCast<uint8_t>(index);
	return result;
}

MetadataPointer MetadataManager::RegisterDiskPointer(MetaBlockPointer pointer) {
	unique_lock<mutex> guard(block_lock);

	auto block_id = pointer.GetBlockId();
	MetadataBlock block;
	block.block_id = block_id;
	AddAndRegisterBlock(guard, std::move(block));
	return FromDiskPointerInternal(guard, pointer);
}

MetadataPointer MetadataManager::RegisterRecoveryDiskPointer(MetaBlockPointer pointer) {
	unique_lock<mutex> guard(block_lock);

	auto block_id = pointer.GetBlockId();
	if (blocks.find(block_id) == blocks.end()) {
		MetadataBlock block;
		block.block_id = block_id;
		AddAndRegisterBlock(guard, std::move(block));
		if (!modified_blocks.emplace(block_id, NumericLimits<idx_t>::Maximum()).second) {
			throw InternalException("Recovered metadata block already has modified-block state");
		}
	} else if (modified_blocks.find(block_id) == modified_blocks.end()) {
		throw InternalException("Recovered metadata block is missing modified-block state");
	}
	return FromDiskPointerInternal(guard, pointer);
}

BlockPointer MetadataManager::ToBlockPointer(MetaBlockPointer meta_pointer, const idx_t metadata_block_size) {
	auto index = meta_pointer.GetBlockIndex();
	if (index >= MetadataManager::METADATA_BLOCK_COUNT) {
		throw IOException("Metadata block index %llu exceeds metadata block count %llu", index,
		                  MetadataManager::METADATA_BLOCK_COUNT);
	}
	BlockPointer result;
	result.block_id = meta_pointer.GetBlockId();
	result.offset = index * NumericCast<uint32_t>(metadata_block_size) + meta_pointer.offset;
	D_ASSERT(result.offset < metadata_block_size * MetadataManager::METADATA_BLOCK_COUNT);
	return result;
}

MetaBlockPointer MetadataManager::FromBlockPointer(BlockPointer block_pointer, const idx_t metadata_block_size) {
	if (!block_pointer.IsValid()) {
		return MetaBlockPointer();
	}
	idx_t index = block_pointer.offset / metadata_block_size;
	auto offset = block_pointer.offset % metadata_block_size;
	if (index >= MetadataManager::METADATA_BLOCK_COUNT) {
		throw IOException("Metadata block offset %llu exceeds metadata block capacity %llu", block_pointer.offset,
		                  metadata_block_size * MetadataManager::METADATA_BLOCK_COUNT);
	}
	D_ASSERT(offset < metadata_block_size);
	MetaBlockPointer result;
	result.block_pointer = idx_t(block_pointer.block_id) | index << 56ULL;
	result.offset = UnsafeNumericCast<uint32_t>(offset);
	return result;
}

idx_t MetadataManager::BlockCount() {
	lock_guard<mutex> guard(block_lock);
	D_ASSERT(blocks.size() >= task_private_blocks.size());
	return blocks.size() - task_private_blocks.size();
}

void MetadataManager::Flush(QueryContext context) {
	vector<block_id_t> block_ids;
	{
		lock_guard<mutex> guard(block_lock);
		block_ids.reserve(blocks.size() - task_private_blocks.size());
		for (auto &entry : blocks) {
			if (task_private_blocks.find(entry.first) == task_private_blocks.end()) {
				block_ids.push_back(entry.first);
			}
		}
	}
	for (auto block_id : block_ids) {
		FlushBlock(block_id, context);
	}
}

void MetadataManager::FlushBlock(block_id_t block_id, QueryContext context) {
	const idx_t total_metadata_size = GetMetadataBlockSize() * METADATA_BLOCK_COUNT;
	shared_ptr<BlockHandle> block_handle;
	{
		lock_guard<mutex> guard(block_lock);
		auto entry = blocks.find(block_id);
		if (entry == blocks.end()) {
			throw InternalException("Cannot flush a missing metadata block");
		}
		auto &block = entry->second;
		if (!block.dirty) {
			if (block.block->BlockId() >= MAXIMUM_BLOCK) {
				throw InternalException("Transient blocks must always be marked as dirty");
			}
			return;
		}
		block_handle = block.block;
	}

	auto handle = buffer_manager.Pin(block_handle);
	memset(handle.GetDataMutable() + total_metadata_size, 0, block_manager.GetBlockSize() - total_metadata_size);
	shared_ptr<BlockHandle> persistent_block;
	if (block_handle->BlockId() >= MAXIMUM_BLOCK) {
		persistent_block = block_manager.ConvertToPersistent(context, block_id, std::move(block_handle),
		                                                     std::move(handle), ConvertToPersistentMode::THREAD_SAFE);
	} else {
		D_ASSERT(block_handle->BlockId() == block_id);
		block_manager.Write(context, handle.GetFileBuffer(), block_id);
		persistent_block = std::move(block_handle);
	}

	lock_guard<mutex> guard(block_lock);
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		throw InternalException("Metadata block disappeared while it was being flushed");
	}
	entry->second.block = std::move(persistent_block);
	entry->second.dirty = false;
}

void MetadataManager::FlushTaskPrivateBlocks(uint64_t owner_id, QueryContext context) {
	auto block_ids = GetTaskPrivateBlockIds(owner_id);
	for (auto block_id : block_ids) {
		FlushBlock(block_id, context);
	}
}

void MetadataManager::Write(WriteStream &sink) {
	lock_guard<mutex> guard(block_lock);
	sink.Write<uint64_t>(blocks.size() - task_private_blocks.size());
	for (auto &kv : blocks) {
		if (task_private_blocks.find(kv.first) == task_private_blocks.end()) {
			kv.second.Write(sink);
		}
	}
}

void MetadataManager::Read(ReadStream &source) {
	auto block_count = source.Read<uint64_t>();
	for (idx_t i = 0; i < block_count; i++) {
		auto block = MetadataBlock::Read(source);

		unique_lock<mutex> guard(block_lock);
		auto entry = blocks.find(block.block_id);
		if (entry == blocks.end()) {
			// block does not exist yet
			AddAndRegisterBlock(guard, std::move(block));
		} else {
			// block was already created - only copy over the free list
			entry->second.free_blocks = std::move(block.free_blocks);
		}
	}
}

void MetadataBlock::Write(WriteStream &sink) {
	sink.Write<block_id_t>(block_id);
	sink.Write<idx_t>(FreeBlocksToInteger());
}

idx_t MetadataManager::GetMetadataBlockSize() const {
	return AlignValueFloor(block_manager.GetBlockSize() / METADATA_BLOCK_COUNT);
}

MetadataBlock MetadataBlock::Read(ReadStream &source) {
	MetadataBlock result;
	result.block_id = source.Read<block_id_t>();
	auto free_list = source.Read<idx_t>();
	result.FreeBlocksFromInteger(free_list);
	return result;
}

idx_t MetadataBlock::FreeBlocksToInteger() {
	idx_t result = 0;
	for (idx_t i = 0; i < free_blocks.size(); i++) {
		D_ASSERT(free_blocks[i] < idx_t(64));
		idx_t mask = idx_t(1) << idx_t(free_blocks[i]);
		result |= mask;
	}
	return result;
}

vector<uint8_t> MetadataBlock::BlocksFromInteger(idx_t free_list) {
	vector<uint8_t> blocks;
	for (idx_t i = 64; i > 0; i--) {
		auto index = i - 1;
		idx_t mask = idx_t(1) << index;
		if (free_list & mask) {
			blocks.push_back(UnsafeNumericCast<uint8_t>(index));
		}
	}
	return blocks;
}

void MetadataBlock::FreeBlocksFromInteger(idx_t free_list) {
	free_blocks.clear();
	if (free_list == 0) {
		return;
	}
	free_blocks = BlocksFromInteger(free_list);
}

void MetadataManager::MarkBlocksAsModified() {
	unique_lock<mutex> guard(block_lock);
	// for any blocks that were modified in the last checkpoint - set them to free blocks currently
	for (auto &kv : modified_blocks) {
		auto block_id = kv.first;
		idx_t modified_list = kv.second;
		auto entry = blocks.find(block_id);
		D_ASSERT(entry != blocks.end());
		auto &block = entry->second;
		idx_t current_free_blocks = block.FreeBlocksToInteger();
		// merge the current set of free blocks with the modified blocks
		idx_t new_free_blocks = current_free_blocks | modified_list;
		if (new_free_blocks == NumericLimits<idx_t>::Maximum()) {
			// if new free_blocks is all blocks - mark entire block as modified
			blocks.erase(entry);

			guard.unlock();
			block_manager.MarkBlockAsModified(block_id);
			guard.lock();
		} else {
			// set the new set of free blocks
			block.FreeBlocksFromInteger(new_free_blocks);
		}
	}

	modified_blocks.clear();

	for (auto &kv : blocks) {
		auto &block = kv.second;
		if (task_private_blocks.find(kv.first) != task_private_blocks.end()) {
			continue;
		}
		idx_t free_list = block.FreeBlocksToInteger();
		idx_t occupied_list = ~free_list;
		modified_blocks[block.block_id] = occupied_list;
	}
}

void MetadataManager::ClearModifiedBlocks(const vector<MetaBlockPointer> &pointers) {
	if (pointers.empty()) {
		return;
	}
	unique_lock<mutex> guard(block_lock);
	for (auto &pointer : pointers) {
		auto block_id = pointer.GetBlockId();
		auto block_index = pointer.GetBlockIndex();
		auto entry = modified_blocks.find(block_id);
		if (entry == modified_blocks.end()) {
			throw InternalException("ClearModifiedBlocks - Block id %llu not found in modified_blocks", block_id);
		}
		auto &modified_list = entry->second;
		// unset the bit
		modified_list &= ~(1ULL << block_index);
	}
}

bool MetadataManager::BlockIsModified(const MetaBlockPointer &pointer) {
	unique_lock<mutex> guard(block_lock);
	auto block_id = pointer.GetBlockId();
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		throw InternalException("BlockIsNotModified - Block id %llu not found in blocks", block_id);
	}
	auto entry2 = modified_blocks.find(block_id);
	return entry2 != modified_blocks.end();
}

bool MetadataManager::BlockHasBeenCleared(const MetaBlockPointer &pointer) {
	unique_lock<mutex> guard(block_lock);
	auto block_id = pointer.GetBlockId();
	auto block_index = pointer.GetBlockIndex();
	auto entry = modified_blocks.find(block_id);
	if (entry == modified_blocks.end()) {
		throw InternalException("BlockHasBeenCleared - Block id %llu not found in modified_blocks", block_id);
	}
	auto &modified_list = entry->second;
	return (modified_list & (1ULL << block_index)) == 0ULL;
}

vector<MetadataBlockInfo> MetadataManager::GetMetadataInfo() const {
	vector<MetadataBlockInfo> result;
	unique_lock<mutex> guard(block_lock);
	for (auto &block : blocks) {
		if (task_private_blocks.find(block.first) != task_private_blocks.end()) {
			continue;
		}
		MetadataBlockInfo block_info;
		block_info.block_id = block.second.block_id;
		block_info.total_blocks = MetadataManager::METADATA_BLOCK_COUNT;
		for (auto free_block : block.second.free_blocks) {
			block_info.free_list.push_back(free_block);
		}
		std::sort(block_info.free_list.begin(), block_info.free_list.end());
		result.push_back(std::move(block_info));
	}
	std::sort(result.begin(), result.end(),
	          [](const MetadataBlockInfo &a, const MetadataBlockInfo &b) { return a.block_id < b.block_id; });
	return result;
}

vector<shared_ptr<BlockHandle>> MetadataManager::GetBlocks() const {
	vector<shared_ptr<BlockHandle>> result;
	unique_lock<mutex> guard(block_lock);
	for (auto &entry : blocks) {
		if (task_private_blocks.find(entry.first) == task_private_blocks.end()) {
			result.push_back(entry.second.block);
		}
	}
	return result;
}

vector<block_id_t> MetadataManager::GetTaskPrivateBlockIds(uint64_t owner_id) const {
	lock_guard<mutex> guard(block_lock);
	auto entry = task_private_owners.find(owner_id);
	if (entry == task_private_owners.end()) {
		throw InternalException("Task-private metadata owner does not exist");
	}
	for (auto block_id : entry->second) {
		auto private_entry = task_private_blocks.find(block_id);
		if (blocks.find(block_id) == blocks.end() || private_entry == task_private_blocks.end() ||
		    private_entry->second != owner_id) {
			throw InternalException("Task-private metadata block ownership is inconsistent");
		}
	}
	auto result = entry->second;
	std::sort(result.begin(), result.end());
	return result;
}

void MetadataManager::PublishTaskPrivateBlocks(uint64_t owner_id) {
	vector<block_id_t> block_ids;
	{
		lock_guard<mutex> guard(block_lock);
		auto owner_entry = task_private_owners.find(owner_id);
		if (owner_entry == task_private_owners.end()) {
			throw InternalException("Task-private metadata owner does not exist");
		}
		block_ids = owner_entry->second;
		for (auto block_id : block_ids) {
			auto block_entry = blocks.find(block_id);
			auto private_entry = task_private_blocks.find(block_id);
			if (block_entry == blocks.end() || private_entry == task_private_blocks.end() ||
			    private_entry->second != owner_id || block_entry->second.dirty ||
			    block_entry->second.block->BlockId() >= MAXIMUM_BLOCK ||
			    modified_blocks.find(block_id) != modified_blocks.end()) {
				throw InternalException("Cannot publish unflushed task-private metadata blocks");
			}
		}
		modified_blocks.reserve(modified_blocks.size() + block_ids.size());
		try {
			for (auto block_id : block_ids) {
				auto occupied_blocks = ~blocks.find(block_id)->second.FreeBlocksToInteger();
				modified_blocks.emplace(block_id, occupied_blocks);
			}
		} catch (...) {
			for (auto block_id : block_ids) {
				modified_blocks.erase(block_id);
			}
			throw;
		}
		for (auto block_id : block_ids) {
			task_private_blocks.erase(block_id);
		}
		task_private_owners.erase(owner_entry);
	}
	for (auto block_id : block_ids) {
		block_manager.MarkBlockAsCheckpointed(block_id);
	}
}

void MetadataManager::AbortTaskPrivateBlocks(uint64_t owner_id) {
	vector<block_id_t> block_ids;
	vector<shared_ptr<BlockHandle>> block_handles;
	{
		lock_guard<mutex> guard(block_lock);
		auto owner_entry = task_private_owners.find(owner_id);
		if (owner_entry == task_private_owners.end()) {
			throw InternalException("Task-private metadata owner does not exist");
		}
		block_ids = owner_entry->second;
		block_handles.reserve(block_ids.size());
		for (auto block_id : block_ids) {
			auto block_entry = blocks.find(block_id);
			auto private_entry = task_private_blocks.find(block_id);
			if (block_entry == blocks.end() || private_entry == task_private_blocks.end() ||
			    private_entry->second != owner_id) {
				throw InternalException("Task-private metadata block ownership is inconsistent");
			}
			block_handles.push_back(block_entry->second.block);
			blocks.erase(block_entry);
			task_private_blocks.erase(block_id);
		}
		task_private_owners.erase(owner_entry);
	}
	block_handles.clear();
	for (auto block_id : block_ids) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

block_id_t MetadataManager::PeekNextBlockId() const {
	return block_manager.PeekFreeBlockId();
}

block_id_t MetadataManager::GetNextBlockId() const {
	return block_manager.GetFreeBlockIdForCheckpoint();
}

} // namespace duckdb
