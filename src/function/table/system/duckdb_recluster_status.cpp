#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/recluster/recluster_status.hpp"

namespace duckdb {

struct DuckDBReclusterStatusEntry {
	string database_name;
	string schema_name;
	string table_name;
	ReclusterTableStatus status;
};

struct DuckDBReclusterStatusData : public GlobalTableFunctionState {
	vector<DuckDBReclusterStatusEntry> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DuckDBReclusterStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_id");
	return_types.emplace_back(LogicalType::UUID);
	names.emplace_back("sort_state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("current_sort_order_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sort_columns");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	names.emplace_back("layout_version");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("current_order_coverage");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("unsorted_row_groups");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("unsorted_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("not_checkpointed_unsorted_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("oldest_unsorted_age");
	return_types.emplace_back(LogicalType::INTERVAL);
	names.emplace_back("run_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("max_overlap_depth");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("p95_overlap_depth");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_recluster_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("largest_run_fraction");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("active_prepare_tasks");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pending_finalize_tasks");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pending_delete_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("prepared_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("retired_layout_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("blocked_reason");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("last_error");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBReclusterStatusInit(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBReclusterStatusData>();
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type != CatalogType::TABLE_ENTRY) {
				return;
			}
			auto &table = entry.Cast<TableCatalogEntry>();
			if (!table.IsDuckTable()) {
				return;
			}
			auto &duck_table = entry.Cast<DuckTableEntry>();
			if (!duck_table.HasSortHistory()) {
				return;
			}
			auto &storage = duck_table.GetStorage();
			auto &attached = storage.GetAttached();
			auto status = attached.GetReclusterManager().GetTableStatus(duck_table);
			result->entries.push_back({attached.GetName().GetIdentifierName(),
			                           duck_table.schema.name.GetIdentifierName(), duck_table.name.GetIdentifierName(),
			                           std::move(status)});
		});
	}
	return std::move(result);
}

static Value ReclusterAgeValue(const optional<int64_t> &age_ms) {
	if (!age_ms) {
		return Value(LogicalType::INTERVAL);
	}
	auto max_ms = NumericLimits<int64_t>::Maximum() / Interval::MICROS_PER_MSEC;
	auto clamped_ms = MinValue<int64_t>(*age_ms, max_ms);
	return Value::INTERVAL(Interval::FromMicro(clamped_ms * Interval::MICROS_PER_MSEC));
}

static Value ReclusterOptionalString(const optional<string> &value) {
	return value ? Value(*value) : Value(LogicalType::VARCHAR);
}

static void DuckDBReclusterStatusFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBReclusterStatusData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		auto &status = entry.status;
		output.data[0].Append(Value(entry.database_name));
		output.data[1].Append(Value(entry.schema_name));
		output.data[2].Append(Value(entry.table_name));
		output.data[3].Append(Value::UUID(status.table_id));
		output.data[4].Append(Value(status.enabled ? "ENABLED" : "DISABLED"));
		output.data[5].Append(Value::UBIGINT(status.current_sort_order_id));
		vector<Value> sort_columns;
		sort_columns.reserve(status.sort_columns.size());
		for (auto &column : status.sort_columns) {
			sort_columns.emplace_back(column);
		}
		output.data[6].Append(Value::LIST(LogicalType::VARCHAR, std::move(sort_columns)));
		output.data[7].Append(Value::UBIGINT(status.layout_version));
		output.data[8].Append(Value::DOUBLE(status.current_order_coverage));
		output.data[9].Append(Value::UBIGINT(status.unsorted_row_groups));
		output.data[10].Append(Value::UBIGINT(status.unsorted_bytes));
		output.data[11].Append(Value::UBIGINT(status.not_checkpointed_unsorted_bytes));
		output.data[12].Append(ReclusterAgeValue(status.oldest_unsorted_age_ms));
		output.data[13].Append(Value::UBIGINT(status.run_count));
		output.data[14].Append(Value::UBIGINT(status.max_overlap_depth));
		output.data[15].Append(Value::UBIGINT(status.p95_overlap_depth));
		output.data[16].Append(Value::UBIGINT(status.remaining_recluster_bytes));
		output.data[17].Append(Value::DOUBLE(status.largest_run_fraction));
		output.data[18].Append(Value::UBIGINT(status.active_prepare_tasks));
		output.data[19].Append(Value::UBIGINT(status.pending_finalize_tasks));
		output.data[20].Append(Value::UBIGINT(status.pending_delete_rows));
		output.data[21].Append(Value::UBIGINT(status.prepared_bytes));
		output.data[22].Append(Value::UBIGINT(status.retired_layout_bytes));
		output.data[23].Append(ReclusterOptionalString(status.blocked_reason));
		output.data[24].Append(ReclusterOptionalString(status.last_error));
		count++;
	}
}

void DuckDBReclusterStatusFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_recluster_status", {}, DuckDBReclusterStatusFunction,
	                              DuckDBReclusterStatusBind, DuckDBReclusterStatusInit));
}

} // namespace duckdb
