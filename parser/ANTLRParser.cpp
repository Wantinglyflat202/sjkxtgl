#include "ANTLRParser.h"
#include "antlr4-runtime.h"
#include <iostream>
#include <sstream>

class SQLErrorListener : public antlr4::BaseErrorListener {
public:
    std::string errorMessage;
    bool hasError = false;
    
    void syntaxError(antlr4::Recognizer* recognizer, 
                     antlr4::Token* offendingSymbol,
                     size_t line, 
                     size_t charPositionInLine,
                     const std::string& msg,
                     std::exception_ptr e) override {
        hasError = true;
        std::ostringstream oss;
        oss << "Syntax error at line " << line << ":" << charPositionInLine << " - " << msg;
        errorMessage = oss.str();
    }
};

SQLStatement ANTLRParser::parse(const std::string& sql) {
    SQLStatement stmt;
    errorMsg.clear();
    
    try {
        std::string sqlWithSemicolon = sql;
        while (!sqlWithSemicolon.empty() && 
               (sqlWithSemicolon.back() == ' ' || sqlWithSemicolon.back() == '\t' ||
                sqlWithSemicolon.back() == '\n' || sqlWithSemicolon.back() == '\r')) {
            sqlWithSemicolon.pop_back();
        }
        if (!sqlWithSemicolon.empty() && sqlWithSemicolon.back() != ';') {
            sqlWithSemicolon += ';';
        }
        
 
        antlr4::ANTLRInputStream input(sqlWithSemicolon);
        
        SQLLexer lexer(&input);
        lexer.removeErrorListeners();
        SQLErrorListener lexerErrorListener;
        lexer.addErrorListener(&lexerErrorListener);
        
        antlr4::CommonTokenStream tokens(&lexer);
        SQLParser parser(&tokens);
        parser.removeErrorListeners();
        SQLErrorListener parserErrorListener;
        parser.addErrorListener(&parserErrorListener);
        
        SQLParser::ProgramContext* tree = parser.program();
        
        if (lexerErrorListener.hasError) {
            stmt.errorMessage = lexerErrorListener.errorMessage;
            errorMsg = lexerErrorListener.errorMessage;
            return stmt;
        }
        
        if (parserErrorListener.hasError) {
            stmt.errorMessage = parserErrorListener.errorMessage;
            errorMsg = parserErrorListener.errorMessage;
            return stmt;
        }
        
        auto statements = tree->statement();
        if (statements.empty()) {
            stmt.errorMessage = "Empty SQL statement";
            errorMsg = stmt.errorMessage;
            return stmt;
        }
        
        auto stmtCtx = statements[0];
    
        if (stmtCtx->Annotation() || stmtCtx->Null()) {
            stmt.valid = true;
            stmt.type = SQLType::UNKNOWN;
            return stmt;
        }
        
        if (stmtCtx->db_statement()) {
            auto dbStmt = stmtCtx->db_statement();
            auto result = dbStmt->accept(&visitor);
            stmt = std::any_cast<SQLStatement>(result);
        } else if (stmtCtx->table_statement()) {
            auto tableStmt = stmtCtx->table_statement();
            auto result = tableStmt->accept(&visitor);
            stmt = std::any_cast<SQLStatement>(result);
        } else if (stmtCtx->alter_statement()) {
            auto alterStmt = stmtCtx->alter_statement();
            auto result = alterStmt->accept(&visitor);
            stmt = std::any_cast<SQLStatement>(result);
        } else {
            stmt.errorMessage = "Unknown statement type";
            errorMsg = stmt.errorMessage;
        }
    } catch (const std::exception& e) {
        stmt.errorMessage = std::string("Parse error: ") + e.what();
        errorMsg = stmt.errorMessage;
    }
    return stmt;
}

