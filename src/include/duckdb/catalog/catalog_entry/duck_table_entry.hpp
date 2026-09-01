//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/duck_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/storage/recluster/table_sort_metadata.hpp"

namespace duckdb {

class CommitDropState;
class DuckTransaction;

struct AddConstraintInfo;
struct CreateTriggerInfo;
struct SetSortedByInfo;

//! A table catalog entry
class DuckTableEntry : public TableCatalogEntry {
public:
	//! Create a TableCatalogEntry and initialize storage for it
	DuckTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
	               shared_ptr<DataTable> inherited_storage = nullptr,
	               shared_ptr<CatalogSet> inherited_triggers = nullptr);

public:
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction, AlterInfo &info) override;
	void PublishAlter(ClientContext &context, CatalogEntry &previous_entry) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	void Rollback(CatalogEntry &prev_entry) override;
	void OnDrop() override;

	//! Returns the underlying storage of the table
	DataTable &GetStorage() override;

	//! Get statistics of a column (physical or virtual) within the table
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const StorageIndex &storage_index);
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	unique_ptr<BlockingSample> GetSample() override;

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CreateInfo> GetInfo() const override;

	const optional<TableSortCatalogMetadata> &GetSortMetadata() const {
		return sort_metadata;
	}
	bool HasSortHistory() const {
		return sort_metadata.has_value();
	}
	bool SortEnabled() const {
		return sort_metadata && sort_metadata->IsEnabled();
	}

	void SetAsRoot() override;

	void CommitAlter(string &column_name, CommitDropState &drop_state);
	void CommitDrop(CommitDropState &drop_state);
	void HoldReclusterDDLWriteGate(DuckTransaction &transaction, const char *operation);

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	vector<ColumnSegmentInfo>
	GetColumnSegmentInfo(const QueryContext &context,
	                     const ColumnSegmentInfoScanOptions &options = ColumnSegmentInfoScanOptions {}) override;
	void InitializeColumnSegmentInfoScan(ColumnSegmentInfoScanState &state) override;
	bool ScanColumnSegmentInfo(const QueryContext &context, ColumnSegmentInfoScanState &state,
	                           vector<ColumnSegmentInfo> &result) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	bool IsDuckTable() const override {
		return true;
	}

	//! Returns the virtual columns for this table
	virtual_column_map_t GetVirtualColumns() const override;

	optional_ptr<CatalogEntry> CreateTrigger(CatalogTransaction transaction, CreateTriggerInfo &info) override;
	void ScanTriggers(CatalogTransaction transaction,
	                  const std::function<void(CatalogEntry &)> &callback) const override;
	//! Scan all triggers without a transaction (used by checkpoint writer)
	void ScanTriggersNonTransactional(const std::function<void(CatalogEntry &)> &callback);
	//! Drop a trigger by name
	bool DropTrigger(CatalogTransaction transaction, const Identifier &name, bool cascade);

private:
	unique_ptr<CatalogEntry> RenameColumn(ClientContext &context, RenameColumnInfo &info);
	unique_ptr<CatalogEntry> RenameField(ClientContext &context, RenameFieldInfo &info);
	unique_ptr<CatalogEntry> AddColumn(ClientContext &context, AddColumnInfo &info);
	unique_ptr<CatalogEntry> AddField(ClientContext &context, AddFieldInfo &info);
	unique_ptr<CatalogEntry> RemoveColumn(ClientContext &context, RemoveColumnInfo &info);
	unique_ptr<CatalogEntry> RemoveField(ClientContext &context, RemoveFieldInfo &info);
	unique_ptr<CatalogEntry> SetDefault(ClientContext &context, SetDefaultInfo &info);
	unique_ptr<CatalogEntry> ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info,
	                                          AlterTableType alter_table_type, AlterTableInfo &post_image_info);
	unique_ptr<CatalogEntry> SetNotNull(ClientContext &context, SetNotNullInfo &info);
	unique_ptr<CatalogEntry> DropNotNull(ClientContext &context, DropNotNullInfo &info);
	unique_ptr<CatalogEntry> AddForeignKeyConstraint(AlterForeignKeyInfo &info);
	unique_ptr<CatalogEntry> DropForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info);
	unique_ptr<CatalogEntry> SetColumnComment(ClientContext &context, SetColumnCommentInfo &info);
	unique_ptr<CatalogEntry> AddConstraint(ClientContext &context, AddConstraintInfo &info);
	unique_ptr<CatalogEntry> SetSortedBy(ClientContext &context, SetSortedByInfo &info);

	void UpdateConstraintsOnColumnDrop(const LogicalIndex &removed_index, const vector<LogicalIndex> &adjusted_indices,
	                                   const RemoveColumnInfo &info, CreateTableInfo &create_info,
	                                   const vector<unique_ptr<BoundConstraint>> &bound_constraints, bool is_generated);

private:
	//! A reference to the underlying storage unit used for this table
	shared_ptr<DataTable> storage;
	//! The catalog set holding triggers for this table
	shared_ptr<CatalogSet> triggers;
	//! Manages dependencies of the individual columns of the table
	ColumnDependencyManager column_dependency_manager;
	//! Persistent sort metadata belonging to this catalog version
	optional<TableSortCatalogMetadata> sort_metadata;
};
} // namespace duckdb
