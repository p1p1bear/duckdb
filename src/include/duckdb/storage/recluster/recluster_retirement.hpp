//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_retirement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/recluster/row_group_layout.hpp"

namespace duckdb {

class AttachedDatabase;
struct DataTableInfo;
class RowGroupCollection;
struct ReclusterRetirementEntry;
struct ReclusterRetirementCheckpointState;

class PreparedReclusterRetirement {
public:
	PreparedReclusterRetirement();
	PreparedReclusterRetirement(PreparedReclusterRetirement &&other) noexcept;
	PreparedReclusterRetirement &operator=(PreparedReclusterRetirement &&other) noexcept;
	~PreparedReclusterRetirement();

	PreparedReclusterRetirement(const PreparedReclusterRetirement &) = delete;
	PreparedReclusterRetirement &operator=(const PreparedReclusterRetirement &) = delete;

	bool IsActive() const noexcept;

private:
	friend class ReclusterRetirementRegistry;

	explicit PreparedReclusterRetirement(unique_ptr<ReclusterRetirementEntry> entry);

private:
	unique_ptr<ReclusterRetirementEntry> entry;
};

class ReclusterRetirementCheckpoint {
public:
	ReclusterRetirementCheckpoint();
	ReclusterRetirementCheckpoint(ReclusterRetirementCheckpoint &&other) noexcept;
	ReclusterRetirementCheckpoint &operator=(ReclusterRetirementCheckpoint &&other) noexcept = delete;
	~ReclusterRetirementCheckpoint();

	ReclusterRetirementCheckpoint(const ReclusterRetirementCheckpoint &) = delete;
	ReclusterRetirementCheckpoint &operator=(const ReclusterRetirementCheckpoint &) = delete;

	bool Empty() const noexcept;
	void Apply() noexcept;
	void HeaderSucceeded() noexcept;

private:
	friend class ReclusterRetirementRegistry;

	explicit ReclusterRetirementCheckpoint(unique_ptr<ReclusterRetirementCheckpointState> state);

private:
	unique_ptr<ReclusterRetirementCheckpointState> state;
};

class ReclusterRetirementRegistry {
public:
	explicit ReclusterRetirementRegistry(AttachedDatabase &db);
	~ReclusterRetirementRegistry();

	PreparedReclusterRetirement PrepareLayoutRetirement(shared_ptr<RowGroupCollection> collection,
	                                                    shared_ptr<const RowGroupLayout> old_layout,
	                                                    RowGroupRange retired_range);
	//! Commit only links an entry fully allocated by PrepareLayoutRetirement.
	void Commit(PreparedReclusterRetirement &&retirement) noexcept;
	ReclusterRetirementCheckpoint PrepareCheckpoint();
	void Cleanup() noexcept;

	idx_t Count() const;
	idx_t GetRetiredBytes(const DataTableInfo &table_info) const;

private:
	friend class ReclusterRetirementCheckpoint;

	void AbortCheckpoint(ReclusterRetirementCheckpointState &state) noexcept;
	void FinishCheckpoint(ReclusterRetirementCheckpointState &state) noexcept;
	void DestroyEntries(unique_ptr<ReclusterRetirementEntry> entries) noexcept;

private:
	AttachedDatabase &db;
	mutable mutex lock;
	unique_ptr<ReclusterRetirementEntry> entries;
};

} // namespace duckdb
