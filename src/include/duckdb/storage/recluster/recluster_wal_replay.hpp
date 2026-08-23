//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/recluster/recluster_wal_replay.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/storage/wal_entry.hpp"

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class ReclusterWALReplayContextState;

class ReclusterWALReplayContext {
public:
	explicit ReclusterWALReplayContext(AttachedDatabase &db);
	~ReclusterWALReplayContext();

	void SetWALCheckpointIteration(uint64_t checkpoint_iteration);
	void ObserveEntry(WALType type);
	void AddHeader(WALReclusterEntry entry);
	void AddDelete(WALReclusterDeleteEntry entry);
	void FinishTransaction(optional_idx transaction_end, optional_ptr<ClientContext> context = nullptr);

	void BeginReplay();
	void OnTransactionCommitted();
	void VerifyReplayComplete(bool all_succeeded) const;
	bool HasUncommittedReclusterTransaction() const;
	void ReleaseUncommittedReservations() noexcept;
	void RetainUncommittedReservationsUntilShutdown() noexcept;
	void ReleaseAllReservations() noexcept;

private:
	unique_ptr<ReclusterWALReplayContextState> state;
};

} // namespace duckdb
