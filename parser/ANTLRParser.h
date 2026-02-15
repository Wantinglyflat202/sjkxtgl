#ifndef ANTLR_PARSER_H
#define ANTLR_PARSER_H

#include "SQLStatement.h"
#include "SQLStatementVisitor.h"
#include "generated/SQLLexer.h"
#include "generated/SQLParser.h"
#include <string>
#include <memory>

class ANTLRParser {
private:
    std::string errorMsg;
    SQLStatementVisitor visitor;

public:
    ANTLRParser() = default;
    SQLStatement parse(const std::string& sql);
    std::string getLastError() const { return errorMsg; }
};
using SimpleParser = ANTLRParser;
#endif

