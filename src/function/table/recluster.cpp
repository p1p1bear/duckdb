#include "duckdb/function/table/range.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/recluster/recluster_manager.hpp"
#include "duckdb/storage/table/data_table_info.hpp"

namespace duckdb {

struct ReclusterFunctionData : public TableFunctionData {
	ReclusterFunctionData(QualifiedName table_name_p, ReclusterExplicitOptions options_p)
	    : table_name(std::move(table_name_p)), options(options_p) {
	}

	QualifiedName table_name;
	ReclusterExplicitOptions options;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ReclusterFunctionData>(table_name, options);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReclusterFunctionData>();
		return table_name == other.table_name && options.create_checkpoint == other.options.create_checkpoint &&
		       options.max_bytes == other.options.max_bytes && options.max_tasks == other.options.max_tasks;
	}
};

struct ReclusterFunctionState : public GlobalTableFunctionState {
	bool finished = false;
};

static idx_t ParseReclusterByteBudget(const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("max_bytes cannot be NULL");
	}
	auto text = value.GetValue<string>();
	if (text.empty() || text[0] == '-' || StringUtil::CIEquals(text, "none") || StringUtil::CIEquals(text, "null")) {
		throw InvalidInputException("max_bytes must be a positive byte size");
	}
	auto result = DBConfig::ParseMemoryLimit(text);
	if (result == 0 || result == DConstants::INVALID_INDEX) {
		throw InvalidInputException("max_bytes must be a positive byte size");
	}
	return result;
}

static unique_ptr<FunctionData> ReclusterBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw BinderException("recluster requires a non-NULL table name");
	}
	auto table_name = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	CatalogEntryRetriever retriever(context);
	table_name = Binder::BindTableName(retriever, table_name);
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, table_name);
	if (entry.type != CatalogType::TABLE_ENTRY || !entry.Cast<TableCatalogEntry>().IsDuckTable()) {
		throw BinderException("recluster can only be used with a DuckDB table");
	}

	ReclusterExplicitOptions options;
	options.max_bytes = DBConfig::ParseMemoryLimit("1GB");
	options.max_tasks = 1;
	for (auto &parameter : input.named_parameters) {
		if (parameter.first == "create_checkpoint") {
			if (parameter.second.IsNull()) {
				throw InvalidInputException("create_checkpoint cannot be NULL");
			}
			options.create_checkpoint = parameter.second.GetValue<bool>();
		} else if (parameter.first == "max_bytes") {
			options.max_bytes = ParseReclusterByteBudget(parameter.second);
		} else if (parameter.first == "max_tasks") {
			if (parameter.second.IsNull()) {
				throw InvalidInputException("max_tasks cannot be NULL");
			}
			options.max_tasks = NumericCast<idx_t>(parameter.second.GetValue<uint64_t>());
			if (options.max_tasks == 0) {
				throw InvalidInputException("max_tasks must be greater than zero");
			}
		}
	}

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("tasks_completed");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("input_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("output_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_recluster_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);
	return make_uniq<ReclusterFunctionData>(std::move(table_name), options);
}

static unique_ptr<GlobalTableFunctionState> ReclusterInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ReclusterFunctionState>();
}

static void ReclusterFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<ReclusterFunctionState>();
	if (state.finished) {
		return;
	}
	auto &bind_data = input.bind_data->Cast<ReclusterFunctionData>();
	auto &table = Catalog::GetEntry<DuckTableEntry>(context, bind_data.table_name);
	auto &manager = table.GetStorage().GetDataTableInfo()->GetDB().GetReclusterManager();
	auto result = manager.RunExplicit(context, bind_data.table_name, bind_data.options);
	state.finished = true;

	output.data[0].Append(Value(result.table_name));
	output.data[1].Append(Value::UBIGINT(result.tasks_completed));
	output.data[2].Append(Value::UBIGINT(result.input_bytes));
	output.data[3].Append(Value::UBIGINT(result.output_bytes));
	output.data[4].Append(Value::UBIGINT(result.remaining_recluster_bytes));
	output.data[5].Append(Value(ReclusterExplicitStateToString(result.state)));
	output.data[6].Append(Value(result.message));
}

void ReclusterTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction function("recluster", {LogicalType::VARCHAR}, ReclusterFunction, ReclusterBind, ReclusterInit);
	function.named_parameters["create_checkpoint"] = LogicalType::BOOLEAN;
	function.named_parameters["max_bytes"] = LogicalType::VARCHAR;
	function.named_parameters["max_tasks"] = LogicalType::UBIGINT;
	set.AddFunction(function);
}

} // namespace duckdb
