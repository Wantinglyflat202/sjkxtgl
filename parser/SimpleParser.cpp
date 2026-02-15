#include "SimpleParser.h"
#include <iostream>
#include <cstdlib>

SimpleParser::SimpleParser() : pos(0) {}

std::string SimpleParser::toUpper(const std::string& s) {
    std::string result = s;
    for (char& c : result) {
        c = std::toupper(c);
    }
    return result;
}

std::vector<std::string> SimpleParser::tokenize(const std::string& sql) {
    std::vector<std::string> result;
    std::string token;
    bool inString = false;
    char stringChar = 0;
    
    for (size_t i = 0; i < sql.length(); i++) {
        char c = sql[i];
        
        if (inString) {
            token += c;
            if (c == stringChar) {
                result.push_back(token);
                token.clear();
                inString = false;
            }
        } else if (c == '\'' || c == '"') {
            if (!token.empty()) {
                result.push_back(token);
                token.clear();
            }
            token += c;
            inString = true;
            stringChar = c;
        } else if (std::isspace(c)) {
            if (!token.empty()) {
                result.push_back(token);
                token.clear();
            }
        } else if (c == ',' || c == '(' || c == ')' || c == ';' || c == '*') {
            if (!token.empty()) {
                result.push_back(token);
                token.clear();
            }
            result.push_back(std::string(1, c));
        } else if (c == '<' || c == '>' || c == '=') {
            if (!token.empty()) {
                result.push_back(token);
                token.clear();
            }
            // 处理 <=, >=, <>, =
            if (i + 1 < sql.length()) {
                char next = sql[i + 1];
                if ((c == '<' && (next == '=' || next == '>')) ||
                    (c == '>' && next == '=')) {
                    result.push_back(std::string(1, c) + std::string(1, next));
                    i++;
                    continue;
                }
            }
            result.push_back(std::string(1, c));
        } else if (c == '.') {
            // 处理 table.column 或浮点数
            if (!token.empty() && std::isdigit(token[0])) {
                // 浮点数
                token += c;
            } else {
                if (!token.empty()) {
                    result.push_back(token);
                    token.clear();
                }
                result.push_back(".");
            }
        } else {
            token += c;
        }
    }
    
    if (!token.empty()) {
        result.push_back(token);
    }
    
    return result;
}

bool SimpleParser::match(const std::string& expected) {
    if (isEnd()) return false;
    return toUpper(tokens[pos]) == toUpper(expected);
}

bool SimpleParser::matchAny(const std::vector<std::string>& expected) {
    for (const auto& e : expected) {
        if (match(e)) return true;
    }
    return false;
}

std::string SimpleParser::current() {
    if (isEnd()) return "";
    return tokens[pos];
}

std::string SimpleParser::consume() {
    if (isEnd()) return "";
    return tokens[pos++];
}

bool SimpleParser::isEnd() {
    return pos >= tokens.size();
}

bool SimpleParser::expect(const std::string& expected) {
    if (!match(expected)) {
        setError("Expected '" + expected + "' but got '" + current() + "'");
        return false;
    }
    consume();
    return true;
}

void SimpleParser::setError(const std::string& msg) {
    errorMsg = msg;
}

SQLStatement SimpleParser::parse(const std::string& sql) {
    tokens = tokenize(sql);
    pos = 0;
    errorMsg.clear();
    
    SQLStatement stmt;
    
    if (tokens.empty()) {
        stmt.errorMessage = "Empty SQL statement";
        return stmt;
    }
    
    std::string firstToken = toUpper(current());
    
    if (firstToken == "CREATE") {
        consume();
        if (match("DATABASE")) {
            stmt = parseCreateDatabase();
        } else if (match("TABLE")) {
            stmt = parseCreateTable();
        } else {
            stmt.errorMessage = "Unknown CREATE statement";
        }
    } else if (firstToken == "DROP") {
        consume();
        if (match("DATABASE")) {
            stmt = parseDropDatabase();
        } else if (match("TABLE")) {
            stmt = parseDropTable();
        } else {
            stmt.errorMessage = "Unknown DROP statement";
        }
    } else if (firstToken == "SHOW") {
        consume();
        if (match("DATABASES")) {
            stmt = parseShowDatabases();
        } else if (match("TABLES")) {
            stmt = parseShowTables();
        } else if (match("INDEXES")) {
            stmt = parseShowIndexes();
        } else {
            stmt.errorMessage = "Unknown SHOW statement";
        }
    } else if (firstToken == "USE") {
        consume();
        stmt = parseUseDatabase();
    } else if (firstToken == "DESC") {
        consume();
        stmt = parseDescTable();
    } else if (firstToken == "INSERT") {
        consume();
        stmt = parseInsert();
    } else if (firstToken == "DELETE") {
        consume();
        stmt = parseDelete();
    } else if (firstToken == "UPDATE") {
        consume();
        stmt = parseUpdate();
    } else if (firstToken == "SELECT") {
        consume();
        stmt = parseSelect();
    } else if (firstToken == "ALTER") {
        consume();
        stmt = parseAlter();
    } else if (firstToken == "LOAD") {
        consume();
        stmt = parseLoadData();
    } else {
        stmt.errorMessage = "Unknown statement: " + firstToken;
    }
    
    return stmt;
}

// ==================== 数据库语句 ====================

SQLStatement SimpleParser::parseCreateDatabase() {
    SQLStatement stmt;
    stmt.type = SQLType::CREATE_DATABASE;
    
    if (!expect("DATABASE")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected database name";
        return stmt;
    }
    
    stmt.databaseName = consume();
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseDropDatabase() {
    SQLStatement stmt;
    stmt.type = SQLType::DROP_DATABASE;
    
    if (!expect("DATABASE")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected database name";
        return stmt;
    }
    
    stmt.databaseName = consume();
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseShowDatabases() {
    SQLStatement stmt;
    stmt.type = SQLType::SHOW_DATABASES;
    
    if (!expect("DATABASES")) return stmt;
    
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseShowTables() {
    SQLStatement stmt;
    stmt.type = SQLType::SHOW_TABLES;
    
    if (!expect("TABLES")) return stmt;
    
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseShowIndexes() {
    SQLStatement stmt;
    stmt.type = SQLType::SHOW_INDEXES;
    
    if (!expect("INDEXES")) return stmt;
    
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseUseDatabase() {
    SQLStatement stmt;
    stmt.type = SQLType::USE_DATABASE;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected database name";
        return stmt;
    }
    
    stmt.databaseName = consume();
    stmt.valid = true;
    return stmt;
}

// ==================== 表语句 ====================

SQLStatement SimpleParser::parseCreateTable() {
    SQLStatement stmt;
    stmt.type = SQLType::CREATE_TABLE;
    
    if (!expect("TABLE")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    
    if (!expect("(")) return stmt;
    
    // 解析字段列表
    while (!isEnd() && !match(")")) {
        if (match("PRIMARY")) {
            // PRIMARY KEY
            consume();  // PRIMARY
            if (!expect("KEY")) return stmt;
            
            std::string keyName;
            if (!match("(")) {
                keyName = consume();  // 可选的约束名
            }
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.primaryKey.columns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.primaryKey.name = keyName;
            
        } else if (match("FOREIGN")) {
            // FOREIGN KEY
            consume();  // FOREIGN
            if (!expect("KEY")) return stmt;
            
            KeyDef fk;
            if (!match("(")) {
                fk.name = consume();
            }
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                fk.columns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            if (!expect("REFERENCES")) return stmt;
            
            fk.refTable = consume();
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                fk.refColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.foreignKeys.push_back(fk);
            
        } else {
            // 普通字段
            ColumnDef col = parseColumnDef();
            if (col.name.empty()) {
                stmt.errorMessage = "Invalid column definition";
                return stmt;
            }
            stmt.columns.push_back(col);
        }
        
        if (match(",")) consume();
    }
    
    if (!expect(")")) return stmt;
    
    stmt.valid = true;
    return stmt;
}

ColumnDef SimpleParser::parseColumnDef() {
    ColumnDef col;
    
    if (isEnd()) return col;
    
    col.name = consume();
    
    int length = 0;
    col.type = parseType(length);
    col.length = length;
    
    // NOT NULL
    if (match("NOT")) {
        consume();
        if (match("NULL")) {
            consume();
            col.notNull = true;
        }
    }
    
    // DEFAULT value
    if (match("DEFAULT")) {
        consume();
        col.hasDefault = true;
        col.defaultValue = parseValue();
    }
    
    return col;
}

DataType SimpleParser::parseType(int& length) {
    length = 0;
    
    if (match("INT")) {
        consume();
        return DataType::INT;
    } else if (match("FLOAT")) {
        consume();
        return DataType::FLOAT;
    } else if (match("VARCHAR")) {
        consume();
        if (expect("(")) {
            if (!isEnd()) {
                length = std::atoi(consume().c_str());
            }
            expect(")");
        }
        return DataType::VARCHAR;
    }
    
    return DataType::UNKNOWN;
}

SQLStatement SimpleParser::parseDropTable() {
    SQLStatement stmt;
    stmt.type = SQLType::DROP_TABLE;
    
    if (!expect("TABLE")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseDescTable() {
    SQLStatement stmt;
    stmt.type = SQLType::DESC_TABLE;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    stmt.valid = true;
    return stmt;
}

// ==================== 数据操作语句 ====================

SQLStatement SimpleParser::parseInsert() {
    SQLStatement stmt;
    stmt.type = SQLType::INSERT;
    
    if (!expect("INTO")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    
    if (!expect("VALUES")) return stmt;
    
    // 解析值列表
    while (!isEnd() && !match(";")) {
        std::vector<Value> values = parseValueList();
        if (!values.empty()) {
            stmt.valueLists.push_back(values);
        }
        if (match(",")) consume();
    }
    
    stmt.valid = !stmt.valueLists.empty();
    return stmt;
}

std::vector<Value> SimpleParser::parseValueList() {
    std::vector<Value> values;
    
    if (!expect("(")) return values;
    
    while (!isEnd() && !match(")")) {
        values.push_back(parseValue());
        if (match(",")) consume();
    }
    
    expect(")");
    return values;
}

Value SimpleParser::parseValue() {
    if (isEnd()) return Value::makeNull();
    
    std::string tok = current();
    
    if (toUpper(tok) == "NULL") {
        consume();
        return Value::makeNull();
    }
    
    // 字符串
    if (tok.length() >= 2 && (tok[0] == '\'' || tok[0] == '"')) {
        consume();
        // 去掉引号
        return Value(tok.substr(1, tok.length() - 2));
    }
    
    // 数字
    consume();
    bool hasDecimal = tok.find('.') != std::string::npos;
    if (hasDecimal) {
        return Value(std::stof(tok));
    } else {
        return Value(std::stoi(tok));
    }
}

SQLStatement SimpleParser::parseDelete() {
    SQLStatement stmt;
    stmt.type = SQLType::DELETE;
    
    if (!expect("FROM")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    
    // WHERE 子句
    if (match("WHERE")) {
        consume();
        stmt.whereClauses = parseWhereAndClauses();
    }
    
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseUpdate() {
    SQLStatement stmt;
    stmt.type = SQLType::UPDATE;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    
    if (!expect("SET")) return stmt;
    
    // 解析 SET 子句
    while (!isEnd() && !match("WHERE") && !match(";")) {
        SetClause sc;
        sc.column = consume();
        if (!expect("=")) return stmt;
        sc.value = parseValue();
        stmt.setClauses.push_back(sc);
        if (match(",")) consume();
    }
    
    // WHERE 子句
    if (match("WHERE")) {
        consume();
        stmt.whereClauses = parseWhereAndClauses();
    }
    
    stmt.valid = true;
    return stmt;
}

SQLStatement SimpleParser::parseSelect() {
    SQLStatement stmt;
    stmt.type = SQLType::SELECT;
    
    // 解析选择器
    if (match("*")) {
        consume();
        Selector sel;
        sel.isAllColumns = true;
        stmt.selectors.push_back(sel);
    } else {
        while (!isEnd() && !match("FROM")) {
            Selector sel = parseSelector();
            stmt.selectors.push_back(sel);
            if (match(",")) consume();
        }
    }
    
    // FROM
    if (!expect("FROM")) return stmt;
    
    // 表列表
    while (!isEnd() && !match("WHERE") && !match("GROUP") && 
           !match("ORDER") && !match("LIMIT") && !match(";")) {
        stmt.fromTables.push_back(consume());
        if (match(",")) consume();
    }
    
    // WHERE
    if (match("WHERE")) {
        consume();
        stmt.whereClauses = parseWhereAndClauses();
    }
    
    // GROUP BY
    if (match("GROUP")) {
        consume();
        if (expect("BY")) {
            stmt.hasGroupBy = true;
            stmt.groupByColumn = parseColumn();
        }
    }
    
    // ORDER BY
    if (match("ORDER")) {
        consume();
        if (expect("BY")) {
            stmt.hasOrderBy = true;
            stmt.orderByColumn = parseColumn();
            if (match("ASC")) {
                consume();
                stmt.orderType = OrderType::ASC;
            } else if (match("DESC")) {
                consume();
                stmt.orderType = OrderType::DESC;
            }
        }
    }
    
    // LIMIT
    if (match("LIMIT")) {
        consume();
        stmt.hasLimit = true;
        stmt.limit = std::atoi(consume().c_str());
        if (match("OFFSET")) {
            consume();
            stmt.offset = std::atoi(consume().c_str());
        }
    }
    
    stmt.valid = true;
    return stmt;
}

Selector SimpleParser::parseSelector() {
    Selector sel;
    
    // 检查聚合函数
    std::string tok = toUpper(current());
    if (tok == "COUNT" || tok == "AVG" || tok == "MAX" || tok == "MIN" || tok == "SUM") {
        if (tok == "COUNT") sel.aggregate = AggregateType::COUNT;
        else if (tok == "AVG") sel.aggregate = AggregateType::AVG;
        else if (tok == "MAX") sel.aggregate = AggregateType::MAX;
        else if (tok == "MIN") sel.aggregate = AggregateType::MIN;
        else if (tok == "SUM") sel.aggregate = AggregateType::SUM;
        
        consume();
        if (expect("(")) {
            if (match("*")) {
                consume();
                sel.isCountStar = true;
            } else {
                sel.column = parseColumn();
            }
            expect(")");
        }
    } else {
        sel.column = parseColumn();
    }
    
    return sel;
}

Column SimpleParser::parseColumn() {
    Column col;
    
    if (isEnd()) return col;
    
    std::string first = consume();
    
    if (match(".")) {
        consume();  // .
        col.tableName = first;
        col.columnName = consume();
    } else {
        col.columnName = first;
    }
    
    return col;
}

std::vector<WhereClause> SimpleParser::parseWhereAndClauses() {
    std::vector<WhereClause> clauses;
    
    while (!isEnd() && !match("GROUP") && !match("ORDER") && !match("LIMIT") && !match(";")) {
        WhereClause wc = parseWhereClause();
        clauses.push_back(wc);
        
        if (match("AND")) {
            consume();
        } else {
            break;
        }
    }
    
    return clauses;
}

WhereClause SimpleParser::parseWhereClause() {
    WhereClause wc;
    
    wc.column = parseColumn();
    
    // IS NULL / IS NOT NULL
    if (match("IS")) {
        consume();
        if (match("NOT")) {
            consume();
            wc.op = CompareOp::IS_NOT_NULL;
        } else {
            wc.op = CompareOp::IS_NULL;
        }
        expect("NULL");
        return wc;
    }
    
    // IN
    if (match("IN")) {
        consume();
        wc.op = CompareOp::IN;
        wc.inList = parseValueList();
        return wc;
    }
    
    // LIKE
    if (match("LIKE")) {
        consume();
        wc.op = CompareOp::LIKE;
        wc.value = parseValue();
        return wc;
    }
    
    // 比较操作符
    wc.op = parseOperator();
    
    // 右侧可能是值或列
    std::string next = current();
    if (!isEnd() && (next[0] == '\'' || next[0] == '"' || 
                     std::isdigit(next[0]) || next[0] == '-' ||
                     toUpper(next) == "NULL")) {
        wc.value = parseValue();
        wc.isColumnCompare = false;
    } else {
        wc.rightColumn = parseColumn();
        wc.isColumnCompare = true;
    }
    
    return wc;
}

CompareOp SimpleParser::parseOperator() {
    std::string tok = current();
    consume();
    
    if (tok == "=") return CompareOp::EQ;
    if (tok == "<>") return CompareOp::NE;
    if (tok == "<") return CompareOp::LT;
    if (tok == "<=") return CompareOp::LE;
    if (tok == ">") return CompareOp::GT;
    if (tok == ">=") return CompareOp::GE;
    
    return CompareOp::EQ;
}

// ==================== ALTER 语句 ====================

SQLStatement SimpleParser::parseAlter() {
    SQLStatement stmt;
    
    if (!expect("TABLE")) return stmt;
    
    if (isEnd()) {
        stmt.errorMessage = "Expected table name";
        return stmt;
    }
    
    stmt.tableName = consume();
    
    if (match("ADD")) {
        consume();
        
        if (match("INDEX")) {
            consume();
            stmt.type = SQLType::ALTER_ADD_INDEX;
            
            // 可选的索引名
            if (!match("(")) {
                stmt.indexName = consume();
            }
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.indexColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.valid = true;
            
        } else if (match("PRIMARY")) {
            consume();
            stmt.type = SQLType::ALTER_ADD_PRIMARY_KEY;
            
            if (!expect("KEY")) return stmt;
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.indexColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.valid = true;
            
        } else if (match("FOREIGN")) {
            consume();
            stmt.type = SQLType::ALTER_ADD_FOREIGN_KEY;
            
            if (!expect("KEY")) return stmt;
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.indexColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            if (!expect("REFERENCES")) return stmt;
            
            stmt.refTableName = consume();
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.refColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.valid = true;
            
        } else if (match("UNIQUE")) {
            consume();
            stmt.type = SQLType::ALTER_ADD_UNIQUE;
            
            if (!match("(")) {
                stmt.indexName = consume();
            }
            
            if (!expect("(")) return stmt;
            
            while (!isEnd() && !match(")")) {
                stmt.indexColumns.push_back(consume());
                if (match(",")) consume();
            }
            
            if (!expect(")")) return stmt;
            stmt.valid = true;
            
        } else if (match("CONSTRAINT")) {
            consume();
            stmt.constraintName = consume();
            
            if (match("PRIMARY")) {
                consume();
                stmt.type = SQLType::ALTER_ADD_PRIMARY_KEY;
                
                if (!expect("KEY")) return stmt;
                if (!expect("(")) return stmt;
                
                while (!isEnd() && !match(")")) {
                    stmt.indexColumns.push_back(consume());
                    if (match(",")) consume();
                }
                
                if (!expect(")")) return stmt;
                stmt.valid = true;
                
            } else if (match("FOREIGN")) {
                consume();
                stmt.type = SQLType::ALTER_ADD_FOREIGN_KEY;
                
                if (!expect("KEY")) return stmt;
                if (!expect("(")) return stmt;
                
                while (!isEnd() && !match(")")) {
                    stmt.indexColumns.push_back(consume());
                    if (match(",")) consume();
                }
                
                if (!expect(")")) return stmt;
                if (!expect("REFERENCES")) return stmt;
                
                stmt.refTableName = consume();
                
                if (!expect("(")) return stmt;
                
                while (!isEnd() && !match(")")) {
                    stmt.refColumns.push_back(consume());
                    if (match(",")) consume();
                }
                
                if (!expect(")")) return stmt;
                stmt.valid = true;
            }
        }
        
    } else if (match("DROP")) {
        consume();
        
        if (match("INDEX")) {
            consume();
            stmt.type = SQLType::ALTER_DROP_INDEX;
            stmt.indexName = consume();
            stmt.valid = true;
            
        } else if (match("PRIMARY")) {
            consume();
            stmt.type = SQLType::ALTER_DROP_PRIMARY_KEY;
            
            if (!expect("KEY")) return stmt;
            
            if (!isEnd() && !match(";")) {
                stmt.constraintName = consume();
            }
            stmt.valid = true;
            
        } else if (match("FOREIGN")) {
            consume();
            stmt.type = SQLType::ALTER_DROP_FOREIGN_KEY;
            
            if (!expect("KEY")) return stmt;
            
            stmt.constraintName = consume();
            stmt.valid = true;
        }
    }
    
    return stmt;
}

SQLStatement SimpleParser::parseLoadData() {
    SQLStatement stmt;
    stmt.type = SQLType::LOAD_DATA;
    
    if (!expect("DATA")) return stmt;
    if (!expect("INFILE")) return stmt;
    
    // 文件名 (可能被单引号或双引号包围)
    std::string fileName = consume();
    if (fileName.length() >= 2 && (fileName[0] == '\'' || fileName[0] == '"')) {
        fileName = fileName.substr(1, fileName.length() - 2);
    }
    stmt.fileName = fileName;
    
    if (!expect("INTO")) return stmt;
    if (!expect("TABLE")) return stmt;
    
    stmt.tableName = consume();
    
    if (!expect("FIELDS")) return stmt;
    if (!expect("TERMINATED")) return stmt;
    if (!expect("BY")) return stmt;
    
    // 分隔符
    std::string delimiter = consume();
    if (delimiter.length() >= 2 && delimiter[0] == '\'') {
        delimiter = delimiter.substr(1, delimiter.length() - 2);
    }
    stmt.delimiter = delimiter;
    
    stmt.valid = true;
    return stmt;
}

