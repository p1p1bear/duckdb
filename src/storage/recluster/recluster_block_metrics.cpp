#include "duckdb/storage/recluster/recluster_block_metrics.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {

class ReclusterBlockCollector : public BlockIdVisitor {
public:
	explicit ReclusterBlockCollector(unordered_set<block_id_t> &blocks_p) : blocks(blocks_p) {
	}

	void Visit(block_id_t block_id) override {
		if (block_id >= 0) {
			has_blocks = true;
			blocks.insert(block_id);
		}
	}

	bool HasBlocks() const {
		return has_blocks;
	}

private:
	unordered_set<block_id_t> &blocks;
	bool has_blocks = false;
};

static bool AddMetadataPointer(unordered_set<block_id_t> &blocks, const MetaBlockPointer &pointer) {
	if (pointer.IsValid()) {
		blocks.insert(pointer.GetBlockId());
		return true;
	}
	return false;
}

bool AddReclusterRowGroupBlocks(RowGroup &row_group, unordered_set<block_id_t> &blocks) {
	ReclusterBlockCollector collector(blocks);
	for (idx_t column_index = 0; column_index < row_group.GetColumnCount(); column_index++) {
		row_group.GetRawColumnData(column_index).VisitBlockIds(collector);
	}
	bool has_blocks = collector.HasBlocks();
	for (auto &pointer : row_group.GetColumnStartPointers()) {
		has_blocks |= AddMetadataPointer(blocks, pointer);
	}
	for (auto &pointer : row_group.GetExtraMetadataBlockPointers()) {
		has_blocks |= AddMetadataPointer(blocks, pointer);
	}
	for (auto &pointer : row_group.GetDeleteStartPointers()) {
		has_blocks |= AddMetadataPointer(blocks, pointer);
	}
	for (auto &pointer : row_group.GetLoadedDeleteStoragePointers()) {
		has_blocks |= AddMetadataPointer(blocks, pointer);
	}
	return has_blocks;
}

idx_t GetReclusterRowGroupTransientBytes(RowGroup &row_group, idx_t physical_rows, bool has_persistent_blocks) {
	auto allocation_size = row_group.GetAllocationSize();
	if (allocation_size > 0 || has_persistent_blocks) {
		return allocation_size;
	}
	return physical_rows;
}

idx_t GetReclusterBlockBytes(const BlockManager &block_manager, const unordered_set<block_id_t> &blocks) {
	auto block_size = block_manager.GetBlockAllocSize();
	if (block_size != 0 && blocks.size() > NumericLimits<idx_t>::Maximum() / block_size) {
		return NumericLimits<idx_t>::Maximum();
	}
	return blocks.size() * block_size;
}

} // namespace duckdb
