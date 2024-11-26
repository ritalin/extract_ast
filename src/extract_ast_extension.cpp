#define DUCKDB_EXTENSION_MAIN

#include "extract_ast_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "json_serializer.hpp"
#include "yyjson.hpp"
#include "magic_enum/magic_enum.hpp"

namespace duckdb {

namespace extract_fn {
    
using namespace duckdb;

class SyntaxErrorStatement: public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::INVALID_STATEMENT;
public:
    std::string error_msg;
public:
    SyntaxErrorStatement(const std::string &&msg): SQLStatement(StatementType::INVALID_STATEMENT), error_msg(msg) {}
public:
	DUCKDB_API std::string ToString() const override { return {}; };
	DUCKDB_API unique_ptr<SQLStatement> Copy() const override {
        return make_uniq<SyntaxErrorStatement>(std::string(this->error_msg));
    };
};

struct SplitQuery {
    idx_t location;
    std::string query;
    unique_ptr<SQLStatement> statement;
};

struct ExtractAstFunctionData: public TableFunctionData {
public:
    Value query;
public:
    ExtractAstFunctionData(const Value &query): query(query) {}
};

struct ExtractAstGlobalState: public GlobalTableFunctionState {
public:
    vector<SplitQuery> statements;
    ::duckdb_yyjson::yyjson_alc *allocator;
    idx_t processed_count;
public:
    ExtractAstGlobalState(vector<SplitQuery> &&statements)
        : statements(std::move(statements)), allocator(::duckdb_yyjson::yyjson_alc_dyn_new()), processed_count(0)
    {
    }

    virtual ~ExtractAstGlobalState() {
        ::duckdb_yyjson::yyjson_alc_dyn_free(this->allocator);
    }
};

static constexpr auto StatementTypeNmaes = magic_enum::enum_entries<StatementType>();

inline static auto BindFunction(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> 
{
    
    Vector vec(LogicalType::VARCHAR, StatementTypeNmaes.size());
    size_t i = 0;

    for (auto& e: StatementTypeNmaes) {
        vec.SetValue(i++, StringUtil::Lower(std::string(e.second)));
    }

    return_types.push_back(LogicalType(LogicalType::ENUM(vec, StatementTypeNmaes.size())));
    return_types.push_back(LogicalType(LogicalType::UBIGINT));
    return_types.push_back(LogicalType(LogicalType::VARCHAR));

    names.push_back("statement_type");
    names.push_back("location");
    names.push_back("ast");

    return make_uniq<ExtractAstFunctionData>(input.inputs[0]);
}

// Copy from `duckdb/src/parser/parser.cpp` 
static auto SplitQueryStringIntoStatements(const std::string &query) -> vector<std::pair<size_t, std::string>> {
	// Break sql string down into sql statements using the tokenizer
	vector<std::pair<size_t, std::string>> query_statements;
	auto tokens = Parser::Tokenize(query);

    vector<SimplifiedToken>::const_iterator iter = tokens.cbegin();

    while (true) {
        if (iter->type == SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT) {
            ++iter;
            continue;
        }

        auto found_iter = std::find_if(iter, tokens.cend(), [&query](const SimplifiedToken& tk) {
            return (tk.type == SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR) && (query[tk.start] == ';');
        });

        if (found_iter == tokens.end()) {
            query_statements.push_back({
                iter->start, query.substr(iter->start)
            });
            break;
        }
        else {
            query_statements.push_back({
                iter->start, query.substr(iter->start, found_iter->start - iter->start)
            });
        }

        iter = std::find_if(found_iter+1, tokens.cend(), [&query](const SimplifiedToken& tk) {
            return (tk.type != SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR) || (query[tk.start] != ';');
        });
        if (iter == tokens.cend()) {
            break;
        }
    }

	return query_statements;
}

inline static auto InitFunction(
    ClientContext &context, TableFunctionInitInput &input) -> unique_ptr<GlobalTableFunctionState> 
{
    auto& bind_data = input.bind_data->Cast<ExtractAstFunctionData>();

    auto queries = SplitQueryStringIntoStatements(bind_data.query.ToString());

    vector<SplitQuery> statements;
    statements.reserve(queries.size());
    
    for (auto& q: queries) {
        try {
            Parser parser;
            parser.ParseQuery(q.second);
            statements.emplace_back(SplitQuery{
                .location = q.first,
                .query = q.second,
                .statement = std::move(parser.statements.front())
            });
        }
        catch (const ParserException& ex) {
            unique_ptr<SQLStatement> stmt = make_uniq<SyntaxErrorStatement>(std::string(ex.what()));
            statements.emplace_back(SplitQuery{
                .location = q.first,
                .query = q.second,
                .statement = std::move(stmt)
            });
        }
    }

    return make_uniq<ExtractAstGlobalState>(std::move(statements));
}

inline static auto ScanQuery(ClientContext &context, TableFunctionInput &data, DataChunk &output) -> void {
    auto& state = data.global_state->Cast<ExtractAstGlobalState>();
    auto& bind_data = data.bind_data->Cast<ExtractAstFunctionData>();
    if (state.processed_count >= state.statements.size()) {
        return;
    }

    auto doc = ::duckdb_yyjson::yyjson_mut_doc_new(state.allocator);
    JsonSerializer ser(doc, false, false, true);

    idx_t row = 0;

    for (auto& stmt: state.statements) {
        output.data[0].SetValue(row, StringUtil::Lower(std::string(magic_enum::enum_name(stmt.statement->type))));
        output.data[1].SetValue(row, Value::Numeric(LogicalTypeId::UBIGINT, stmt.location));

        if (stmt.statement->type == StatementType::SELECT_STATEMENT) {
            stmt.statement->Cast<SelectStatement>().Serialize(ser);
            output.data[2].SetValue(row, JSONCommon::WriteVal(ser.GetRootObject(), state.allocator));
        }
        else if (stmt.statement->type == StatementType::INVALID_STATEMENT) {
            output.data[2].SetValue(row, stmt.statement->Cast<SyntaxErrorStatement>().error_msg);
        }
        else {
            output.data[2].SetValue(row, Value());
        }
        ++row;
    }
    output.SetCardinality(row);
    state.processed_count += row;
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
