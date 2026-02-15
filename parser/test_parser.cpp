/*
 * test_parser.cpp
 * 
 * SQL 解析器测试程序
 */

#include "SimpleParser.h"
#include <iostream>
#include <string>

using namespace std;

// 辅助函数：将 SQLType 转换为字符串
string sqlTypeToString(SQLType type) {
    switch (type) {
        case SQLType::CREATE_DATABASE: return "CREATE_DATABASE";
        case SQLType::DROP_DATABASE: return "DROP_DATABASE";
        case SQLType::SHOW_DATABASES: return "SHOW_DATABASES";
        case SQLType::USE_DATABASE: return "USE_DATABASE";
        case SQLType::SHOW_TABLES: return "SHOW_TABLES";
        case SQLType::SHOW_INDEXES: return "SHOW_INDEXES";
        case SQLType::CREATE_TABLE: return "CREATE_TABLE";
        case SQLType::DROP_TABLE: return "DROP_TABLE";
        case SQLType::DESC_TABLE: return "DESC_TABLE";
        case SQLType::LOAD_DATA: return "LOAD_DATA";
        case SQLType::INSERT: return "INSERT";
        case SQLType::DELETE: return "DELETE";
        case SQLType::UPDATE: return "UPDATE";
        case SQLType::SELECT: return "SELECT";
        case SQLType::ALTER_ADD_INDEX: return "ALTER_ADD_INDEX";
        case SQLType::ALTER_DROP_INDEX: return "ALTER_DROP_INDEX";
        case SQLType::ALTER_ADD_PRIMARY_KEY: return "ALTER_ADD_PRIMARY_KEY";
        case SQLType::ALTER_DROP_PRIMARY_KEY: return "ALTER_DROP_PRIMARY_KEY";
        case SQLType::ALTER_ADD_FOREIGN_KEY: return "ALTER_ADD_FOREIGN_KEY";
        case SQLType::ALTER_DROP_FOREIGN_KEY: return "ALTER_DROP_FOREIGN_KEY";
        case SQLType::ALTER_ADD_UNIQUE: return "ALTER_ADD_UNIQUE";
        default: return "UNKNOWN";
    }
}

string dataTypeToString(DataType type) {
    switch (type) {
        case DataType::INT: return "INT";
        case DataType::FLOAT: return "FLOAT";
        case DataType::VARCHAR: return "VARCHAR";
        default: return "UNKNOWN";
    }
}

string valueToString(const Value& v) {
    if (v.isNull) return "NULL";
    switch (v.type) {
        case Value::Type::INT: return to_string(v.intVal);
        case Value::Type::FLOAT: return to_string(v.floatVal);
        case Value::Type::STRING: return "'" + v.strVal + "'";
        default: return "NULL";
    }
}

void printStatement(const SQLStatement& stmt) {
    cout << "类型: " << sqlTypeToString(stmt.type) << endl;
    cout << "有效: " << (stmt.valid ? "是" : "否") << endl;
    
    if (!stmt.valid) {
        cout << "错误: " << stmt.errorMessage << endl;
        return;
    }
    
    if (!stmt.databaseName.empty()) {
        cout << "数据库: " << stmt.databaseName << endl;
    }
    
    if (!stmt.tableName.empty()) {
        cout << "表名: " << stmt.tableName << endl;
    }
    
    // CREATE TABLE
    if (!stmt.columns.empty()) {
        cout << "列定义:" << endl;
        for (const auto& col : stmt.columns) {
            cout << "  - " << col.name << " " << dataTypeToString(col.type);
            if (col.type == DataType::VARCHAR) {
                cout << "(" << col.length << ")";
            }
            if (col.notNull) cout << " NOT NULL";
            if (col.hasDefault) cout << " DEFAULT " << valueToString(col.defaultValue);
            cout << endl;
        }
    }
    
    // PRIMARY KEY
    if (!stmt.primaryKey.columns.empty()) {
        cout << "主键: ";
        for (size_t i = 0; i < stmt.primaryKey.columns.size(); i++) {
            if (i > 0) cout << ", ";
            cout << stmt.primaryKey.columns[i];
        }
        cout << endl;
    }
    
    // INSERT VALUES
    if (!stmt.valueLists.empty()) {
        cout << "插入值:" << endl;
        for (const auto& row : stmt.valueLists) {
            cout << "  (";
            for (size_t i = 0; i < row.size(); i++) {
                if (i > 0) cout << ", ";
                cout << valueToString(row[i]);
            }
            cout << ")" << endl;
        }
    }
    
    // SELECT
    if (!stmt.selectors.empty()) {
        cout << "选择: ";
        for (size_t i = 0; i < stmt.selectors.size(); i++) {
            if (i > 0) cout << ", ";
            const auto& sel = stmt.selectors[i];
            if (sel.isAllColumns) {
                cout << "*";
            } else if (sel.isCountStar) {
                cout << "COUNT(*)";
            } else if (sel.aggregate != AggregateType::NONE) {
                switch (sel.aggregate) {
                    case AggregateType::COUNT: cout << "COUNT"; break;
                    case AggregateType::AVG: cout << "AVG"; break;
                    case AggregateType::MAX: cout << "MAX"; break;
                    case AggregateType::MIN: cout << "MIN"; break;
                    case AggregateType::SUM: cout << "SUM"; break;
                    default: break;
                }
                cout << "(" << sel.column.toString() << ")";
            } else {
                cout << sel.column.toString();
            }
        }
        cout << endl;
    }
    
    if (!stmt.fromTables.empty()) {
        cout << "FROM: ";
        for (size_t i = 0; i < stmt.fromTables.size(); i++) {
            if (i > 0) cout << ", ";
            cout << stmt.fromTables[i];
        }
        cout << endl;
    }
    
    // WHERE
    if (!stmt.whereClauses.empty()) {
        cout << "WHERE条件: " << stmt.whereClauses.size() << " 个" << endl;
    }
    
    // SET
    if (!stmt.setClauses.empty()) {
        cout << "SET:" << endl;
        for (const auto& sc : stmt.setClauses) {
            cout << "  " << sc.column << " = " << valueToString(sc.value) << endl;
        }
    }
    
    // GROUP BY
    if (stmt.hasGroupBy) {
        cout << "GROUP BY: " << stmt.groupByColumn.toString() << endl;
    }
    
    // ORDER BY
    if (stmt.hasOrderBy) {
        cout << "ORDER BY: " << stmt.orderByColumn.toString() 
             << (stmt.orderType == OrderType::ASC ? " ASC" : " DESC") << endl;
    }
    
    // LIMIT
    if (stmt.hasLimit) {
        cout << "LIMIT: " << stmt.limit;
        if (stmt.offset > 0) cout << " OFFSET " << stmt.offset;
        cout << endl;
    }
    
    // ALTER INDEX
    if (!stmt.indexName.empty()) {
        cout << "索引名: " << stmt.indexName << endl;
    }
    if (!stmt.indexColumns.empty()) {
        cout << "索引列: ";
        for (size_t i = 0; i < stmt.indexColumns.size(); i++) {
            if (i > 0) cout << ", ";
            cout << stmt.indexColumns[i];
        }
        cout << endl;
    }
}

void testSQL(SimpleParser& parser, const string& sql) {
    cout << "\n========================================" << endl;
    cout << "SQL: " << sql << endl;
    cout << "----------------------------------------" << endl;
    
    SQLStatement stmt = parser.parse(sql);
    printStatement(stmt);
}

int main() {
    SimpleParser parser;
    
    cout << "========== SQL 解析器测试 ==========" << endl;
    
    // 测试数据库语句
    testSQL(parser, "CREATE DATABASE testdb;");
    testSQL(parser, "DROP DATABASE testdb;");
    testSQL(parser, "SHOW DATABASES;");
    testSQL(parser, "USE testdb;");
    testSQL(parser, "SHOW TABLES;");
    testSQL(parser, "SHOW INDEXES;");
    
    // 测试表语句
    testSQL(parser, "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50), age INT DEFAULT 0, PRIMARY KEY (id));");
    testSQL(parser, "DROP TABLE users;");
    testSQL(parser, "DESC users;");
    
    // 测试 INSERT
    testSQL(parser, "INSERT INTO users VALUES (1, 'Alice', 25);");
    testSQL(parser, "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30);");
    
    // 测试 DELETE
    testSQL(parser, "DELETE FROM users WHERE id = 1;");
    testSQL(parser, "DELETE FROM users WHERE age > 20 AND name = 'Alice';");
    
    // 测试 UPDATE
    testSQL(parser, "UPDATE users SET age = 26 WHERE id = 1;");
    testSQL(parser, "UPDATE users SET name = 'Charlie', age = 35 WHERE id = 2;");
    
    // 测试 SELECT
    testSQL(parser, "SELECT * FROM users;");
    testSQL(parser, "SELECT id, name FROM users WHERE age > 20;");
    testSQL(parser, "SELECT COUNT(*) FROM users;");
    testSQL(parser, "SELECT AVG(age) FROM users;");
    testSQL(parser, "SELECT * FROM users ORDER BY age DESC;");
    testSQL(parser, "SELECT * FROM users LIMIT 10 OFFSET 5;");
    testSQL(parser, "SELECT * FROM users WHERE name LIKE 'A%';");
    testSQL(parser, "SELECT * FROM users WHERE id IN (1, 2, 3);");
    testSQL(parser, "SELECT * FROM users WHERE name IS NOT NULL;");
    
    // 测试 ALTER
    testSQL(parser, "ALTER TABLE users ADD INDEX idx_name (name);");
    testSQL(parser, "ALTER TABLE users DROP INDEX idx_name;");
    testSQL(parser, "ALTER TABLE users ADD PRIMARY KEY (id);");
    testSQL(parser, "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users (id);");
    
    // 测试 LOAD DATA
    testSQL(parser, "LOAD DATA INFILE 'data.csv' INTO TABLE users FIELDS TERMINATED BY ',';");
    
    cout << "\n========== 测试完成 ==========" << endl;
    
    // 交互模式
    cout << "\n进入交互模式，输入 SQL 语句（输入 'quit' 退出）:" << endl;
    string line;
    while (true) {
        cout << "\nsql> ";
        getline(cin, line);
        
        if (line == "quit" || line == "exit") {
            break;
        }
        
        if (line.empty()) continue;
        
        SQLStatement stmt = parser.parse(line);
        printStatement(stmt);
    }
    
    return 0;
}

