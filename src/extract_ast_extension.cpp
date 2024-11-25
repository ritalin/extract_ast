#define DUCKDB_EXTENSION_MAIN

#include "extract_ast_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "json_serializer.hpp"
#include "yyjson.hpp"

namespace duckdb {

namespace extract_fn {
    
using namespace duckdb;

struct ExtractAstFunctionData: public TableFunctionData {
public:
    Value query;
public:
    ExtractAstFunctionData(const Value &query): query(query) {}
};

struct ExtractAstGlobalState: public GlobalTableFunctionState {
public:
    vector<unique_ptr<SQLStatement>> statements;
    ::duckdb_yyjson::yyjson_alc *allocator;
    idx_t processed_count;
public:
    ExtractAstGlobalState(vector<unique_ptr<SQLStatement>> &&statements)
        : statements(std::move(statements)), allocator(::duckdb_yyjson::yyjson_alc_dyn_new()), processed_count(0)
    {
    }

    virtual ~ExtractAstGlobalState() {
        ::duckdb_yyjson::yyjson_alc_dyn_free(this->allocator);
    }
};

inline static auto BindFunction(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> 
{
    // return_types.push_back(LogicalType(LogicalType::Enum()));
    return_types.push_back(LogicalType(LogicalType::VARCHAR));

    // names.push_back("statement_type");
    names.push_back("ast");

    return make_uniq<ExtractAstFunctionData>(input.inputs[0]);
}

inline static auto InitFunction(
    ClientContext &context, TableFunctionInitInput &input) -> unique_ptr<GlobalTableFunctionState> 
{
    auto& bind_data = input.bind_data->Cast<ExtractAstFunctionData>();
    Parser parser;
    parser.ParseQuery(bind_data.query.ToString());

    return make_uniq<ExtractAstGlobalState>(std::move(parser.statements));
}

inline static auto ScanQuery(ClientContext &context, TableFunctionInput &data, DataChunk &output) -> void {
    auto& state = data.global_state->Cast<ExtractAstGlobalState>();
    if (state.processed_count >= state.statements.size()) {
        return;
    }

    auto doc = ::duckdb_yyjson::yyjson_mut_doc_new(state.allocator);
    JsonSerializer ser(doc, false, false, true);

    idx_t i = 0;

    for (auto& stmt: state.statements) {
        if (stmt->type == StatementType::SELECT_STATEMENT) {
            stmt->Cast<SelectStatement>().Serialize(ser);

            output.data[0].SetValue(i++, JSONCommon::WriteVal(ser.GetRootObject(), state.allocator));
        }
    }
    output.SetCardinality(i);
    state.processed_count += i;
}

}

struct ExtractAstFunction {
    static auto Create() -> TableFunction {
        auto entry = TableFunction(
            "extract_ast", 
            {LogicalType(LogicalType::VARCHAR)}, 
            extract_fn::ScanQuery, 
            extract_fn::BindFunction, 
            extract_fn::InitFunction
        );

        return entry;
    }
};

static void LoadInternal(DuckDB &db) {
	Connection conn(db);
	auto &client_context = *conn.context;
	auto &catalog = Catalog::GetSystemCatalog(client_context);
	conn.BeginTransaction();

    CreateTableFunctionInfo fn_info(ExtractAstFunction::Create());
    catalog.CreateTableFunction(client_context, fn_info);

	conn.Commit();
}

void ExtractAstExtension::Load(DuckDB &db) {
	LoadInternal(db);
}
std::string ExtractAstExtension::Name() {
	return "extract_ast";
}

std::string ExtractAstExtension::Version() const {
#ifdef EXT_VERSION_EXTRACT_AST
	return EXT_VERSION_EXTRACT_AST;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void extract_ast_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ExtractAstExtension>();
}

DUCKDB_EXTENSION_API const char *extract_ast_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
