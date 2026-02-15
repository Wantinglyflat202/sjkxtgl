#ifndef SQL_STATEMENT_H
#define SQL_STATEMENT_H
#include <string>
#include <vector>
#include <map>

enum class SQLType {
    UNKNOWN,
    CREATE_DATABASE,
    DROP_DATABASE,
    SHOW_DATABASES,
    USE_DATABASE,
    SHOW_TABLES,
    SHOW_INDEXES,

    CREATE_TABLE,
    DROP_TABLE,
    DESC_TABLE,
    LOAD_DATA,
    INSERT,
    DELETE,
    UPDATE,
    SELECT,
    ALTER_ADD_INDEX,
    ALTER_DROP_INDEX,
    ALTER_ADD_PRIMARY_KEY,
    ALTER_DROP_PRIMARY_KEY,
    ALTER_ADD_FOREIGN_KEY,
    ALTER_DROP_FOREIGN_KEY,
    ALTER_ADD_UNIQUE
};
enum class DataType {
    INT,
    VARCHAR,
    FLOAT,
    UNKNOWN
};
enum class CompareOp {
    EQ,  
    NE,    
    LT,   
    LE, 
    GT,    
    GE,     
    LIKE,
    IS_NULL,
    IS_NOT_NULL,
    IN
};
enum class AggregateType {
    NONE,
    COUNT,
    AVG,
    MAX,
    MIN,
    SUM
};
enum class OrderType {
    ASC,
    DESC
};
struct Value {
    enum class Type { INT, FLOAT, STRING, NULL_VALUE } type;
    int intVal;
    double floatVal;  // 使用 double 以获得更高精度
    std::string strVal;
    bool isNull;
    Value() : type(Type::NULL_VALUE), intVal(0), floatVal(0), isNull(true) {}
    Value(int v) : type(Type::INT), intVal(v), floatVal(0), isNull(false) {}
    Value(double v) : type(Type::FLOAT), intVal(0), floatVal(v), isNull(false) {}
    Value(double v, const std::string& raw) : type(Type::FLOAT), intVal(0), floatVal(v), strVal(raw), isNull(false) {}
    Value(const std::string& v) : type(Type::STRING), intVal(0), floatVal(0), strVal(v), isNull(false) {}
    static Value makeNull() {
        Value v;
        v.isNull = true;
        v.type = Type::NULL_VALUE;
        return v;
    }
};
struct ColumnDef {
    std::string name;
    DataType type;
    int length;        
    bool notNull;
    bool hasDefault;
    Value defaultValue;
    
    ColumnDef() : type(DataType::UNKNOWN), length(0), notNull(false), hasDefault(false) {}
};
struct Column {
    std::string tableName;  
    std::string columnName;
    Column() {}
    Column(const std::string& col) : columnName(col) {}
    Column(const std::string& table, const std::string& col) : tableName(table), columnName(col) {}
    std::string toString() const {
        if (tableName.empty()) return columnName;
        return tableName + "." + columnName;
    }
};
struct WhereClause {
    Column column;
    CompareOp op;
    Value value;            
    Column rightColumn;    
    std::vector<Value> inList;  
    bool isColumnCompare;  
    WhereClause() : op(CompareOp::EQ), isColumnCompare(false) {}
};
struct Selector {
    Column column;
    AggregateType aggregate;
    bool isAllColumns;      
    bool isCountStar; 
    Selector() : aggregate(AggregateType::NONE), isAllColumns(false), isCountStar(false) {}
};
struct KeyDef {
    std::string name;           
    std::vector<std::string> columns;
    std::string refTable;    
    std::vector<std::string> refColumns;
};
struct SetClause {
    std::string column;
    Value value;
};
struct SQLStatement {
    SQLType type;
    bool valid;
    std::string errorMessage;
    std::string databaseName;
    std::string tableName;
    std::vector<ColumnDef> columns;
    KeyDef primaryKey;
    std::vector<KeyDef> foreignKeys;
    std::vector<std::vector<Value>> valueLists;
    std::vector<Selector> selectors;
    std::vector<std::string> fromTables;
    std::vector<WhereClause> whereClauses;
    Column groupByColumn;
    Column orderByColumn;
    OrderType orderType;
    int limit;
    int offset;
    bool hasGroupBy;
    bool hasOrderBy;
    bool hasLimit;
    
    std::vector<SetClause> setClauses;
    std::string indexName;
    std::string constraintName;
    std::vector<std::string> indexColumns;
    std::string refTableName;
    std::vector<std::string> refColumns;
    
    std::string fileName;
    std::string delimiter;
    
    SQLStatement() : 
        type(SQLType::UNKNOWN), 
        valid(false),
        orderType(OrderType::ASC),
        limit(-1),
        offset(0),
        hasGroupBy(false),
        hasOrderBy(false),
        hasLimit(false) {}
    
    bool isValid() const { return valid; }
    std::string getError() const { return errorMessage; }
};
#endif 

