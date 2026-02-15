#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "../system/SystemManager.h"
#include "../parser/SQLStatement.h"
#include <string>
#include <vector>
#include <map>
#include <functional>
#include <algorithm>
#include <sstream>
#include <iomanip>

struct ResultRow {
    std::vector<Value> values;

    Value& operator[](size_t index) { return values[index]; }
    const Value& operator[](size_t index) const { return values[index]; }
    size_t size() const { return values.size(); }
};
struct ResultSet {
    std::vector<std::string> columnNames;
    std::vector<DataType> columnTypes;
    std::vector<ResultRow> rows;
    std::string message;
    bool success;
    int affectedRows;
    ResultSet() : success(true), affectedRows(0) {}
    void addColumn(const std::string& name, DataType type) {
        columnNames.push_back(name);
        columnTypes.push_back(type);
    }
    void addRow(const ResultRow& row) {
        rows.push_back(row);
    }
    void setError(const std::string& msg) {
        success = false;
        message = msg;
    }
    void setMessage(const std::string& msg) {
        message = msg;
    }
    std::string toString() const {
        std::ostringstream oss;
        if (!success) {
            oss << "Error: " << message << std::endl;
            return oss.str();
        }
        if (!message.empty() && rows.empty()) {
            oss << message << std::endl;
            if (affectedRows > 0) {
                oss << "Affected rows: " << affectedRows << std::endl;
            }
            return oss.str();
        }
        if (columnNames.empty()) {
            return message.empty() ? "Empty result set\n" : message + "\n";
        }
        std::vector<size_t> widths(columnNames.size());
        for (size_t i = 0; i < columnNames.size(); i++) {
            widths[i] = columnNames[i].length();
        }
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size() && i < widths.size(); i++) {
                std::string valStr = valueToString(row[i]);
                widths[i] = std::max(widths[i], valStr.length());
            }
        }
        oss << "+";
        for (size_t w : widths) {
            oss << std::string(w + 2, '-') << "+";
        }
        oss << std::endl;
        oss << "|";
        for (size_t i = 0; i < columnNames.size(); i++) {
            oss << " " << std::setw(widths[i]) << std::left << columnNames[i] << " |";
        }
        oss << std::endl;
        oss << "+";
        for (size_t w : widths) {
            oss << std::string(w + 2, '-') << "+";
        }
        oss << std::endl;
        for (const auto& row : rows) {
            oss << "|";
            for (size_t i = 0; i < row.size() && i < widths.size(); i++) {
                std::string valStr = valueToString(row[i]);
                oss << " " << std::setw(widths[i]) << std::left << valStr << " |";
            }
            oss << std::endl;
        }
        oss << "+";
        for (size_t w : widths) {
            oss << std::string(w + 2, '-') << "+";
        }
        oss << std::endl;
        oss << rows.size() << " row(s) in set" << std::endl;
        return oss.str();
    }
    static std::string valueToString(const Value& val) {
        if (val.isNull) return "NULL";
        switch (val.type) {
            case Value::Type::INT:
                return std::to_string(val.intVal);
            case Value::Type::FLOAT: {
                char buf[64];
                snprintf(buf, sizeof(buf), "%.2f", val.floatVal);
                return std::string(buf);
            }
            case Value::Type::STRING:
                {
                    std::string result;
                    result.reserve(val.strVal.length());
                    for (unsigned char c : val.strVal) {
                        if (c >= 32 && c < 127) {
                            result += c;
                        } else if (c >= 128) {
                            result += c;
                        }
                    }
                    return result;
                }
            default:
                return "NULL";
        }
    }
};
class QueryExecutor {
private:
    SystemManager* systemManager;

    std::vector<char> serializeRecord(const TableMeta& meta, const std::vector<Value>& values);
    std::vector<Value> deserializeRecord(const TableMeta& meta, const char* data, int dataLen);

    bool matchWhereClause(const WhereClause& clause, const TableMeta& meta,
                          const std::vector<Value>& record);
    bool matchAllWhereClauses(const std::vector<WhereClause>& clauses, const TableMeta& meta,
                              const std::vector<Value>& record);

    int compareValues(const Value& v1, const Value& v2);
    bool evaluateCompare(CompareOp op, int cmpResult);
    bool likeMatch(const std::string& str, const std::string& pattern);

    Value calculateAggregate(AggregateType aggType, const std::vector<Value>& values);

    ResultSet executeJoin(const std::vector<std::string>& tables,
                          const std::vector<WhereClause>& whereClauses,
                          const std::vector<Selector>& selectors);

    std::vector<std::pair<int, std::vector<Value>>> scanTable(const std::string& tableName);

    std::vector<std::pair<int, std::vector<Value>>> scanTableFiltered(
        const std::string& tableName,
        const std::vector<WhereClause>& whereClauses);

    std::vector<std::pair<int, std::vector<Value>>> indexScan(const std::string& tableName,
                                                               const WhereClause& clause);

    bool shouldUseIndex(const std::string& tableName, const WhereClause& clause);

public:
    QueryExecutor(SystemManager* sm);

    ResultSet executeInsert(const std::string& tableName,const std::vector<std::vector<Value>>& valueLists);


    ResultSet executeDelete(const std::string& tableName,const std::vector<WhereClause>& whereClauses);


    ResultSet executeUpdate(const std::string& tableName,const std::vector<SetClause>& setClauses,const std::vector<WhereClause>& whereClauses);


    ResultSet executeSelect(const std::vector<Selector>& selectors,
                            const std::vector<std::string>& fromTables,
                            const std::vector<WhereClause>& whereClauses,
                            const Column& groupByColumn,
                            const Column& orderByColumn,
                            OrderType orderType,
                            int limit,
                            int offset,
                            bool hasGroupBy,
                            bool hasOrderBy);


    ResultSet executeLoadData(const std::string& fileName,
                              const std::string& tableName,
                              const std::string& delimiter);

    bool checkPrimaryKey(const std::string& tableName,
                         const std::vector<Value>& values);

    bool checkForeignKey(const std::string& tableName,
                         const std::vector<Value>& values);

    bool checkNotNull(const std::string& tableName,
                      const std::vector<Value>& values);
};

#endif

