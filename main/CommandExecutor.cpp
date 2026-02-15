#include "CommandExecutor.h"
#include "../filesystem/utils/MyBitMap.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <set>
#include <cstring>
static std::string getHelpMessage() {
    std::ostringstream oss;
    oss << "Available commands:\n";
    oss << "  Database operations:\n";
    oss << "    CREATE DATABASE dbname   - Create a new database\n";
    oss << "    DROP DATABASE dbname     - Delete a database\n";
    oss << "    SHOW DATABASES           - List all databases\n";
    oss << "    USE dbname               - Select a database\n";
    oss << "\n";
    oss << "  Table operations:\n";
    oss << "    CREATE TABLE name (...)  - Create a new table\n";
    oss << "    DROP TABLE name          - Delete a table\n";
    oss << "    SHOW TABLES              - List all tables\n";
    oss << "    DESC tablename           - Show table structure\n";
    oss << "\n";
    oss << "  Data operations:\n";
    oss << "    INSERT INTO table VALUES (...)\n";
    oss << "    DELETE FROM table WHERE ...\n";
    oss << "    UPDATE table SET ... WHERE ...\n";
    oss << "    SELECT ... FROM ... WHERE ...\n";
    oss << "\n";
    oss << "  Index operations:\n";
    oss << "    ALTER TABLE t ADD INDEX (col)\n";
    oss << "    ALTER TABLE t DROP INDEX name\n";
    oss << "    SHOW INDEXES\n";
    oss << "\n";
    oss << "  Other:\n";
    oss << "    LOAD DATA INFILE 'file' INTO TABLE t FIELDS TERMINATED BY ','\n";
    oss << "    EXIT / QUIT              - Exit the program\n";
    oss << "    HELP                     - Show this message\n";
    return oss.str();
}
CommandExecutor::CommandExecutor(const std::string& dataDir, bool batch) 
    : running(true), batchMode(batch) {
    MyBitMap::initConst();
    fileManager = std::make_unique<FileManager>();
    bufPageManager = std::make_unique<BufPageManager>(fileManager.get());
    systemManager = std::make_unique<SystemManager>(fileManager.get(), bufPageManager.get(), dataDir);
    queryExecutor = std::make_unique<QueryExecutor>(systemManager.get());
}
CommandExecutor::~CommandExecutor() {
    flush();
}
void CommandExecutor::flush() {
    if (systemManager) {
        systemManager->flush();
    }
}
std::string CommandExecutor::getCurrentDatabase() const {
    return systemManager ? systemManager->getCurrentDatabase() : "";
}
std::string CommandExecutor::formatBatch(const ResultSet& result) {
    std::ostringstream oss;
    if (!result.success) {
        oss << "!ERROR\n";
        oss << result.message << "\n";
        oss << "@\n";
        return oss.str();
    }
    if (!result.columnNames.empty()) {
        for (size_t i = 0; i < result.columnNames.size(); i++) {
            if (i > 0) oss << ",";
            oss << result.columnNames[i];
        }
        oss << "\n";
        for (const auto& row : result.rows) {
            for (size_t i = 0; i < row.size(); i++) {
                if (i > 0) oss << ",";
                const Value& val = row[i];
                if (val.isNull) {
                    oss << "NULL";
                } else if (val.type == Value::Type::INT) {
                    oss << val.intVal;
                } else if (val.type == Value::Type::FLOAT) {
                    oss << std::fixed << std::setprecision(2) << val.floatVal;
                } else {
                    oss << val.strVal;
                }
            }
            oss << "\n";
        }
    } else if (!result.message.empty() && result.message.find("rows") != std::string::npos) {
        // 对于 LOAD DATA 等返回行数的操作，输出特殊格式
        oss << result.message << "\n";
    }
    // 其他成功操作（如 USE, CREATE, DROP 等）不输出任何内容，只输出 @
    oss << "@\n";
    return oss.str();
}
std::string CommandExecutor::formatInteractive(const ResultSet& result) {
    return result.toString();
}

std::string CommandExecutor::execute(const std::string& sql) {
    std::string trimmedSql = sql;
    size_t start = trimmedSql.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return batchMode ? "@\n" : "";
    }
    trimmedSql = trimmedSql.substr(start);
    size_t end = trimmedSql.find_last_not_of(" \t\n\r;");
    if (end != std::string::npos) {
        trimmedSql = trimmedSql.substr(0, end + 1);
    }
    
    if (trimmedSql.empty()) {
        return batchMode ? "@\n" : "";
    }
    std::string upperSql = trimmedSql;
    for (char& c : upperSql) c = std::toupper(c);
    if (upperSql == "EXIT" || upperSql == "QUIT") {
        running = false;
        return batchMode ? "@\n" : "Bye\n";
    }
    if (upperSql == "HELP") {
        return batchMode ? "@\n" : getHelpMessage();
    }
    SQLStatement stmt = parser.parse(trimmedSql);
    if (!stmt.isValid()) {
        ResultSet errResult;
        errResult.setError(stmt.errorMessage.empty() ? parser.getLastError() : stmt.errorMessage);
        return batchMode ? formatBatch(errResult) : formatInteractive(errResult);
    }
    ResultSet result;
    switch (stmt.type) {
        case SQLType::CREATE_DATABASE:
        case SQLType::DROP_DATABASE:
        case SQLType::SHOW_DATABASES:
        case SQLType::USE_DATABASE:
        case SQLType::CREATE_TABLE:
        case SQLType::DROP_TABLE:
        case SQLType::SHOW_TABLES:
        case SQLType::SHOW_INDEXES:
        case SQLType::DESC_TABLE:
            result = executeDDL(stmt);
            break;
        case SQLType::INSERT:
        case SQLType::DELETE:
        case SQLType::UPDATE:
        case SQLType::SELECT:
        case SQLType::LOAD_DATA:
            result = executeDML(stmt);
            break;
        case SQLType::ALTER_ADD_INDEX:
        case SQLType::ALTER_DROP_INDEX:
        case SQLType::ALTER_ADD_PRIMARY_KEY:
        case SQLType::ALTER_DROP_PRIMARY_KEY:
        case SQLType::ALTER_ADD_FOREIGN_KEY:
        case SQLType::ALTER_DROP_FOREIGN_KEY:
        case SQLType::ALTER_ADD_UNIQUE:
            result = executeAlter(stmt);
            break;
        default:
            result.setError("Unknown statement type");
            break;
    }
    if (stmt.type == SQLType::DESC_TABLE && result.success) {
        TableMeta meta = systemManager->describeTable(stmt.tableName);
        if (batchMode) {
            return formatDescBatch(meta);
        } else {
            return formatDescInteractive(meta);
        }
    }
    return batchMode ? formatBatch(result) : formatInteractive(result);
}
ResultSet CommandExecutor::executeDDL(const SQLStatement& stmt) {
    ResultSet result;
    switch (stmt.type) {
        case SQLType::CREATE_DATABASE: {
            if (systemManager->createDatabase(stmt.databaseName)) {
                result.setMessage("Database '" + stmt.databaseName + "' created");
            } else {
                result.setError("Failed to create database '" + stmt.databaseName + "'");
            }
            break;
        }
        case SQLType::DROP_DATABASE: {
            if (systemManager->dropDatabase(stmt.databaseName)) {
                result.setMessage("Database '" + stmt.databaseName + "' dropped");
            } else {
                result.setError("Failed to drop database '" + stmt.databaseName + "'");
            }
            break;
        }
        case SQLType::SHOW_DATABASES: {
            auto databases = systemManager->showDatabases();
            result.addColumn("DATABASES", DataType::VARCHAR);
            for (const auto& db : databases) {
                ResultRow row;
                row.values.push_back(Value(db));
                result.addRow(row);
            }
            break;
        }
        case SQLType::USE_DATABASE: {
            if (systemManager->useDatabase(stmt.databaseName)) {
                queryExecutor = std::make_unique<QueryExecutor>(systemManager.get());
                result.setMessage("Database changed to '" + stmt.databaseName + "'");
            } else {
                result.setError("Database '" + stmt.databaseName + "' does not exist");
            }
            break;
        }
        case SQLType::CREATE_TABLE: {
            if (systemManager->getCurrentDatabase().empty()) {
                result.setError("No database selected");
                break;
            }
            std::set<std::string> columnNames;
            std::string duplicateColumn;
            for (const auto& col : stmt.columns) {
                if (columnNames.find(col.name) != columnNames.end()) {
                    duplicateColumn = col.name;
                    break;
                }
                columnNames.insert(col.name);
            }
            if (!duplicateColumn.empty()) {
                result.setError("Duplicate column name: '" + duplicateColumn + "'");
                break;
            }
            std::vector<std::string> pkCols;
            for (const auto& pk : stmt.primaryKey.columns) {
                pkCols.push_back(pk);
            }
            if (systemManager->createTable(stmt.tableName, stmt.columns, pkCols, stmt.foreignKeys)) {
                result.setMessage("Table '" + stmt.tableName + "' created");
            } else {
                result.setError("Failed to create table - primary key constraint error");
            }
            break;
        }
        case SQLType::DROP_TABLE: {
            if (systemManager->getCurrentDatabase().empty()) {
                result.setError("No database selected");
                break;
            }
            if (systemManager->dropTable(stmt.tableName)) {
                result.setMessage("Table '" + stmt.tableName + "' dropped");
            } else {
                result.setError("Failed to drop table '" + stmt.tableName + "'");
            }
            break;
        }
        case SQLType::SHOW_TABLES: {
            if (systemManager->getCurrentDatabase().empty()) {
                result.setError("No database selected");
                break;
            } 
            auto tables = systemManager->showTables();
            result.addColumn("TABLES", DataType::VARCHAR);
            for (const auto& table : tables) {
                ResultRow row;
                row.values.push_back(Value(table));
                result.addRow(row);
            }
            break;
        }
        case SQLType::SHOW_INDEXES: {
            if (systemManager->getCurrentDatabase().empty()) {
                result.setError("No database selected");
                break;
            }
            auto indexes = systemManager->showIndexes();
            result.addColumn("INDEXES", DataType::VARCHAR);
            for (const auto& idx : indexes) {
                ResultRow row;
                row.values.push_back(Value(idx));
                result.addRow(row);
            }
            break;
        }
        case SQLType::DESC_TABLE: {
            if (systemManager->getCurrentDatabase().empty()) {
                result.setError("No database selected");
                break;
            }
            TableMeta meta = systemManager->describeTable(stmt.tableName);
            if (meta.tableName.empty()) {
                result.setError("Table '" + stmt.tableName + "' does not exist");
                break;
            }
            result.addColumn("Field", DataType::VARCHAR);
            result.addColumn("Type", DataType::VARCHAR);
            result.addColumn("Null", DataType::VARCHAR);
            result.addColumn("Default", DataType::VARCHAR);
            for (const auto& col : meta.columns) {
                ResultRow row;
                row.values.push_back(Value(col.name));
                std::string typeStr;
                if (col.type == DataType::INT) {
                    typeStr = "INT";
                } else if (col.type == DataType::FLOAT) {
                    typeStr = "FLOAT";
                } else {
                    typeStr = "VARCHAR(" + std::to_string(col.length) + ")";
                }
                row.values.push_back(Value(typeStr));
                row.values.push_back(Value(col.notNull ? "NO" : "YES"));
                row.values.push_back(Value("NULL"));
                result.addRow(row);
            }
            break;
        }
        default:
            result.setError("Unknown DDL statement");
            break;
    }
    return result;
}
ResultSet CommandExecutor::executeDML(const SQLStatement& stmt) {
    ResultSet result;
    if (systemManager->getCurrentDatabase().empty()) {
        result.setError("No database selected");
        return result;
    }
    switch (stmt.type) {
        case SQLType::INSERT:
            result = queryExecutor->executeInsert(stmt.tableName, stmt.valueLists);

            if (batchMode && result.success) {
                ResultSet rowsResult;
                rowsResult.addColumn("rows", DataType::INT);
                ResultRow row;
                row.values.push_back(Value(result.affectedRows));
                rowsResult.addRow(row);
                return rowsResult;
            }
            break;
        
        case SQLType::DELETE:
            result = queryExecutor->executeDelete(stmt.tableName, stmt.whereClauses);
            if (batchMode && result.success) {
                ResultSet rowsResult;
                rowsResult.addColumn("rows", DataType::INT);
                ResultRow row;
                row.values.push_back(Value(result.affectedRows));
                rowsResult.addRow(row);
                return rowsResult;
            }
            break; 
        case SQLType::UPDATE:
            result = queryExecutor->executeUpdate(stmt.tableName, stmt.setClauses, stmt.whereClauses);
            if (batchMode && result.success) {
                ResultSet rowsResult;
                rowsResult.addColumn("rows", DataType::INT);
                ResultRow row;
                row.values.push_back(Value(result.affectedRows));
                rowsResult.addRow(row);
                return rowsResult;
            }
            break;
        case SQLType::SELECT:
            result = queryExecutor->executeSelect(
                stmt.selectors,
                stmt.fromTables,
                stmt.whereClauses,
                stmt.groupByColumn,
                stmt.orderByColumn,
                stmt.orderType,
                stmt.limit,
                stmt.offset,
                stmt.hasGroupBy,
                stmt.hasOrderBy
            );
            break;       
        case SQLType::LOAD_DATA:
            result = queryExecutor->executeLoadData(stmt.fileName, stmt.tableName, stmt.delimiter);
            if (batchMode && result.success) {
                ResultSet rowsResult;
                rowsResult.addColumn("rows", DataType::INT);
                ResultRow row;
                row.values.push_back(Value(result.affectedRows));
                rowsResult.addRow(row);
                // 复制消息（包含失败信息）以便后续显示
                rowsResult.message = result.message;
                return rowsResult;
            }
            break;        
        default:
            result.setError("Unknown DML statement");
            break;
    }    
    return result;
}
ResultSet CommandExecutor::executeAlter(const SQLStatement& stmt) {
    ResultSet result;   
    if (systemManager->getCurrentDatabase().empty()) {
        result.setError("No database selected");
        return result;
    }
    switch (stmt.type) {
        case SQLType::ALTER_ADD_INDEX: {
            if (stmt.indexColumns.empty()) {
                result.setError("No columns specified for index");
                break;
            }
            
            // xz只支持单列索引
            std::string colName = stmt.indexColumns[0];
            std::string idxName = stmt.indexName.empty() ? 
                                   stmt.tableName + "_" + colName + "_idx" : stmt.indexName;
            if (systemManager->createIndex(stmt.tableName, colName, idxName)) {
                result.setMessage("Index created on " + stmt.tableName + "(" + colName + ")");
            } else {
                result.setError("Failed to create index");
            }
            break;
        }     
        case SQLType::ALTER_DROP_INDEX: {
            if (systemManager->dropIndex(stmt.tableName, stmt.indexName)) {
                result.setMessage("Index '" + stmt.indexName + "' dropped");
            } else {
                result.setError("Failed to drop index '" + stmt.indexName + "'");
            }
            break;
        }      
        case SQLType::ALTER_ADD_PRIMARY_KEY: {
            // 检查表是否已有主键
            TableMeta* meta = systemManager->getTableMeta(stmt.tableName);
            if (!meta) {
                result.setError("Table does not exist");
                break;
            }
            if (!meta->primaryKey.empty()) {
                result.setError("Failed to add primary key - primary key already exists");
                break;
            }
            
            // 使用 QueryExecutor 扫描表并检查是否有重复值
            bool hasDuplicates = false;
            {
                // 获取所有记录并检查主键列是否有重复
                std::set<std::string> seenValues;
                
                // 执行一个 SELECT 来获取所有记录
                std::vector<Selector> selectors;
                for (const auto& pkCol : stmt.indexColumns) {
                    Selector sel;
                    sel.column.columnName = pkCol;
                    selectors.push_back(sel);
                }
                
                ResultSet selectResult = queryExecutor->executeSelect(
                    selectors, {stmt.tableName}, {}, Column(), Column(), 
                    OrderType::ASC, -1, 0, false, false);
                
                for (const auto& row : selectResult.rows) {
                    std::string key;
                    for (const auto& val : row.values) {
                        if (val.isNull) {
                            key += "NULL|";
                        } else if (val.type == Value::Type::INT) {
                            key += std::to_string(val.intVal) + "|";
                        } else if (val.type == Value::Type::FLOAT) {
                            key += std::to_string(val.floatVal) + "|";
                        } else {
                            key += val.strVal + "|";
                        }
                    }
                    if (seenValues.count(key)) {
                        hasDuplicates = true;
                        break;
                    }
                    seenValues.insert(key);
                }
            }
            
            if (hasDuplicates) {
                result.setError("Duplicate entry - duplicate key value violates constraint");
                break;
            }
            
            if (systemManager->addPrimaryKey(stmt.tableName, stmt.indexColumns)) {
                // 成功时不设置消息，让 formatBatch 只输出 @
            } else {
                result.setError("Failed to add primary key - invalid columns");
            }
            break;
        } 
        case SQLType::ALTER_DROP_PRIMARY_KEY: {
            if (systemManager->dropPrimaryKey(stmt.tableName)) {
                result.setMessage("Primary key dropped");
            } else {
                result.setError("Failed to drop primary key - no primary key exists");
            }
            break;
        }
        case SQLType::ALTER_ADD_FOREIGN_KEY: {
            KeyDef fk;
            fk.name = stmt.constraintName.empty() ? 
                       stmt.tableName + "_fk_" + std::to_string(rand()) : stmt.constraintName;
            fk.columns = stmt.indexColumns;
            fk.refTable = stmt.refTableName;
            fk.refColumns = stmt.refColumns;
            
            // 检查现有数据是否满足外键约束
            TableMeta* meta = systemManager->getTableMeta(stmt.tableName);
            TableMeta* refMeta = systemManager->getTableMeta(fk.refTable);
            if (!meta || !refMeta) {
                result.setError("Foreign key references invalid table");
                break;
            }
            
            // 使用 QueryExecutor 获取引用表的所有值
            std::set<std::string> refValues;
            {
                std::vector<Selector> refSelectors;
                for (const auto& refCol : fk.refColumns) {
                    Selector sel;
                    sel.column.columnName = refCol;
                    refSelectors.push_back(sel);
                }
                
                ResultSet refResult = queryExecutor->executeSelect(
                    refSelectors, {fk.refTable}, {}, Column(), Column(),
                    OrderType::ASC, -1, 0, false, false);
                
                for (const auto& row : refResult.rows) {
                    std::string key;
                    for (const auto& val : row.values) {
                        if (val.isNull) {
                            key += "NULL|";
                        } else if (val.type == Value::Type::INT) {
                            key += std::to_string(val.intVal) + "|";
                        } else if (val.type == Value::Type::FLOAT) {
                            key += std::to_string(val.floatVal) + "|";
                        } else {
                            key += val.strVal + "|";
                        }
                    }
                    refValues.insert(key);
                }
            }
            
            // 使用 QueryExecutor 获取当前表的外键列值
            bool hasInvalidRef = false;
            {
                std::vector<Selector> selectors;
                for (const auto& col : fk.columns) {
                    Selector sel;
                    sel.column.columnName = col;
                    selectors.push_back(sel);
                }
                
                ResultSet selectResult = queryExecutor->executeSelect(
                    selectors, {stmt.tableName}, {}, Column(), Column(),
                    OrderType::ASC, -1, 0, false, false);
                
                for (const auto& row : selectResult.rows) {
                    std::string key;
                    for (const auto& val : row.values) {
                        if (val.isNull) {
                            key += "NULL|";
                        } else if (val.type == Value::Type::INT) {
                            key += std::to_string(val.intVal) + "|";
                        } else if (val.type == Value::Type::FLOAT) {
                            key += std::to_string(val.floatVal) + "|";
                        } else {
                            key += val.strVal + "|";
                        }
                    }
                    
                    if (!refValues.count(key)) {
                        hasInvalidRef = true;
                        break;
                    }
                }
            }
            
            if (hasInvalidRef) {
                result.setError("Foreign key constraint failed - referenced values do not exist");
                break;
            }
            
            if (systemManager->addForeignKey(stmt.tableName, fk)) {
                // 成功时不设置消息
            } else {
                result.setError("Failed to add foreign key");
            }
            break;
        }
        
        case SQLType::ALTER_DROP_FOREIGN_KEY: {
            if (systemManager->dropForeignKey(stmt.tableName, stmt.constraintName)) {
                result.setMessage("Foreign key dropped");
            } else {
                result.setError("Failed to drop foreign key");
            }
            break;
        }
        case SQLType::ALTER_ADD_UNIQUE: {
            if (!stmt.indexColumns.empty()) {
                std::string colName = stmt.indexColumns[0];
                if (systemManager->createIndex(stmt.tableName, colName, 
                    stmt.indexName.empty() ? stmt.tableName + "_" + colName + "_uniq" : stmt.indexName)) {
                    result.setMessage("Unique constraint added");
                } else {
                    result.setError("Failed to add unique constraint - duplicate values exist");
                }
            } else {
                result.setError("No columns specified for unique constraint");
            }
            break;
        }
        default:
            result.setError("Unknown ALTER statement");
            break;
    }
    return result;
}
std::string CommandExecutor::formatDescBatch(const TableMeta& meta) {
    std::ostringstream oss;
    oss << "Field,Type,Null,Default\n";

    for (const auto& col : meta.columns) {
        oss << col.name << ",";
        if (col.type == DataType::INT) {
            oss << "INT";
        } else if (col.type == DataType::FLOAT) {
            oss << "FLOAT";
        } else {
            oss << "VARCHAR(" << col.length << ")";
        }
        oss << ",";
        oss << (col.notNull ? "NO" : "YES") << ",";
        oss << "NULL\n";
    }
    oss << "\n";
    if (!meta.primaryKeyColumns.empty()) {
        oss << "PRIMARY KEY (";
        for (size_t i = 0; i < meta.primaryKeyColumns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << meta.primaryKeyColumns[i];
        }
        oss << ");\n";
    }
    for (const auto& fk : meta.foreignKeys) {
        oss << "FOREIGN KEY ";
        if (!fk.name.empty()) {
            oss << fk.name;
        }
        oss << "(";
        for (size_t i = 0; i < fk.columns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << fk.columns[i];
        }
        oss << ") REFERENCES " << fk.refTable << "(";
        for (size_t i = 0; i < fk.refColumns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << fk.refColumns[i];
        }
        oss << ");\n";
    }
    for (const auto& idx : meta.explicitIndexes) {
        oss << "INDEX (";
        for (size_t i = 0; i < idx.columns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << idx.columns[i];
        }
        oss << ");\n";
    }
    oss << "@\n";
    return oss.str();
}
std::string CommandExecutor::formatDescInteractive(const TableMeta& meta) {
    std::ostringstream oss;

    size_t nameWidth = 5;
    size_t typeWidth = 4;  
    
    for (const auto& col : meta.columns) {
        nameWidth = std::max(nameWidth, col.name.length());
        std::string typeStr;
        if (col.type == DataType::INT) {
            typeStr = "INT";
        } else if (col.type == DataType::FLOAT) {
            typeStr = "FLOAT";
        } else {
            typeStr = "VARCHAR(" + std::to_string(col.length) + ")";
        }
        typeWidth = std::max(typeWidth, typeStr.length());
    }
    auto printSeparator = [&]() {
        oss << "+" << std::string(nameWidth + 2, '-') << "+"
            << std::string(typeWidth + 2, '-') << "+"
            << std::string(6, '-') << "+"
            << std::string(9, '-') << "+\n";
    };
    printSeparator();
    oss << "| " << std::setw(nameWidth) << std::left << "Field" << " | "
        << std::setw(typeWidth) << std::left << "Type" << " | "
        << std::setw(4) << "Null" << " | "
        << std::setw(7) << "Default" << " |\n";
    printSeparator();
    for (const auto& col : meta.columns) {
        std::string typeStr;
        if (col.type == DataType::INT) {
            typeStr = "INT";
        } else if (col.type == DataType::FLOAT) {
            typeStr = "FLOAT";
        } else {
            typeStr = "VARCHAR(" + std::to_string(col.length) + ")";
        }
        oss << "| " << std::setw(nameWidth) << std::left << col.name << " | "
            << std::setw(typeWidth) << std::left << typeStr << " | "
            << std::setw(4) << (col.notNull ? "NO" : "YES") << " | "
            << std::setw(7) << "NULL" << " |\n";
    }
    printSeparator();
    if (!meta.primaryKeyColumns.empty()) {
        oss << "PRIMARY KEY (";
        for (size_t i = 0; i < meta.primaryKeyColumns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << meta.primaryKeyColumns[i];
        }
        oss << ");\n";
    }
    for (const auto& fk : meta.foreignKeys) {
        oss << "FOREIGN KEY ";
        if (!fk.name.empty()) {
            oss << fk.name;
        }
        oss << "(";
        for (size_t i = 0; i < fk.columns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << fk.columns[i];
        }
        oss << ") REFERENCES " << fk.refTable << "(";
        for (size_t i = 0; i < fk.refColumns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << fk.refColumns[i];
        }
        oss << ");\n";
    }
    for (const auto& idx : meta.explicitIndexes) {
        oss << "INDEX (";
        for (size_t i = 0; i < idx.columns.size(); i++) {
            if (i > 0) oss << ", ";
            oss << idx.columns[i];
        }
        oss << ");\n";
    }
    return oss.str();
}
std::string CommandExecutor::executeFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        if (batchMode) {
            return "!ERROR\nCannot open file '" + filePath + "'\n@\n";
        }
        return "Error: Cannot open file '" + filePath + "'\n";
    }
    std::ostringstream result;
    std::string line;
    std::string currentStatement;
    while (std::getline(file, line)) {
        size_t start = line.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        if (line[start] == '-' && start + 1 < line.length() && line[start + 1] == '-') continue;
        currentStatement += line + " ";
        if (line.find(';') != std::string::npos) {
            result << execute(currentStatement);
            currentStatement.clear();
        }
    }
    if (!currentStatement.empty()) {
        result << execute(currentStatement);
    }
    file.close();
    return result.str();
}
void CommandExecutor::runInteractive() {
    std::cout << "Welcome to ThisDB. Type 'HELP' for help, 'EXIT' to quit.\n";
    std::cout << std::endl;
    std::string line;
    std::string currentStatement;
    while (running) {

        if (currentStatement.empty()) {
            std::string db = getCurrentDatabase();
            if (db.empty()) {
                std::cout << "sql> ";
            } else {
                std::cout << db << "> ";
            }
        } else {
            std::cout << "  -> ";
        }
        
        if (!std::getline(std::cin, line)) {
            break;
        }
        currentStatement += line + " ";
        size_t semicolonPos = currentStatement.find(';');
        if (semicolonPos != std::string::npos || 
            line == "EXIT" || line == "exit" ||
            line == "QUIT" || line == "quit" ||
            line == "HELP" || line == "help") {         
            std::string result = execute(currentStatement);
            std::cout << result;
            currentStatement.clear();
        }
    }
    std::cout << "Sayonara!\n";
}
