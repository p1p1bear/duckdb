//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_output_writer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/recluster/recluster_types.hpp"

namespace duckdb {

class BlockManager;
class RangeTask;
class RowGroup;
class RowGroupCollection;
struct PersistentCollectionData;

class ReclusterOutput {
public:
	~ReclusterOutput();

	const shared_ptr<RowGroupCollection> &GetCollection() const {
		return collection;
	}
	const PersistentCollectionData &GetPersistentData() const;
	const vector<block_id_t> &GetBlockIds() const {
		return block_ids;
	}
	vector<shared_ptr<RowGroup>> GetRowGroups() const;

	sort_order_id_t GetSortOrderId() const {
		return sort_order_id;
	}
	sort_run_id_t GetRunId() const {
		return run_id;
	}
	idx_t GetRowCount() const {
		return row_count;
	}

	void MarkPublished();
	void Abort();

private:
	friend class ReclusterOutputWriter;

	ReclusterOutput(BlockManager &block_manager, shared_ptr<RowGroupCollection> collection,
	                unique_ptr<PersistentCollectionData> persistent_data, sort_order_id_t sort_order_id,
	                sort_run_id_t run_id, idx_t row_count);
	void AdoptTaskPrivateBlocks(vector<block_id_t> block_ids);

private:
	BlockManager &block_manager;
	shared_ptr<RowGroupCollection> collection;
	unique_ptr<PersistentCollectionData> persistent_data;
	vector<block_id_t> block_ids;
	sort_order_id_t sort_order_id;
	sort_run_id_t run_id;
	idx_t row_count;
	bool owns_blocks = false;
};

class ReclusterOutputWriter {
public:
	explicit ReclusterOutputWriter(RangeTask &task);

	void Write();

private:
	void CheckTask() const;

private:
	RangeTask &task;
};

} // namespace duckdb
