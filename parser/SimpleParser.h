#ifndef SIMPLE_PARSER_H
#define SIMPLE_PARSER_H

#include "SQLStatement.h"
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cctype>

/*
 * 简单 SQL 解析器
 * 手写实现，无需 ANTLR4 依赖
 * 支持基本的 SQL 语句解析
 */
class SimpleParser {
private:
    std::vector<std::string> tokens;
    size_t pos;
    std::string errorMsg;
    
    // 词法分析
    std::vector<std::string> tokenize(const std::string& sql);
    
    // 辅助函数
    std::string toUpper(const std::string& s);
    bool match(const std::string& expected);
    bool matchAny(const std::vector<std::string>& expected);
    std::string current();
    std::string consume();
    bool isEnd();
    bool expect(const std::string& expected);
    void setError(const std::string& msg);
    
    // 解析各种语句
    SQLStatement parseCreateDatabase();
    SQLStatement parseDropDatabase();
    SQLStatement parseShowDatabases();
    SQLStatement parseShowTables();
    SQLStatement parseShowIndexes();
    SQLStatement parseUseDatabase();
    SQLStatement parseCreateTable();
    SQLStatement parseDropTable();
    SQLStatement parseDescTable();
    SQLStatement parseInsert();
    SQLStatement parseDelete();
    SQLStatement parseUpdate();
    SQLStatement parseSelect();
    SQLStatement parseAlter();
    SQLStatement parseLoadData();
    
    // 解析子结构
    ColumnDef parseColumnDef();
    DataType parseType(int& length);
    Value parseValue();
    std::vector<Value> parseValueList();
    WhereClause parseWhereClause();
    std::vector<WhereClause> parseWhereAndClauses();
    Column parseColumn();
    Selector parseSelector();
    CompareOp parseOperator();
    
public:
    SimpleParser();
    
    /*
     * 解析 SQL 语句
     * @param sql: SQL 语句字符串
     * @return: SQLStatement 结构
     */
    SQLStatement parse(const std::string& sql);
    
    /*
     * 获取最后的错误信息
     */
    std::string getLastError() const { return errorMsg; }
};

#endif // SIMPLE_PARSER_H

