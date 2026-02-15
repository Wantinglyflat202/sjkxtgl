#ifndef SYSTEM_MANAGER_H
#define SYSTEM_MANAGER_H

#include "../filesystem/bufmanager/BufPageManager.h"
#include "../filesystem/fileio/FileManager.h"
#include "../record/RecordManager.h"
#include "../index/IndexManager.h"
#include "../parser/SQLStatement.h"
#include <string>
#include <vector>
#include <map>
#include <memory>

struct IndexInfo {
    std::string name;
    std::vector<std::string> columns;
    bool isExplicit;
    bool isUnique;
    IndexInfo() : isExplicit(true), isUnique(false) {}
};
struct TableMeta {
    std::string tableName;
    std::vector<ColumnDef> columns;
    std::vector<std::string> primaryKey;
    std::vector<std::string> primaryKeyColumns;
    std::vector<KeyDef> foreignKeys;
    std::vector<std::string> indexes;
    std::vector<IndexInfo> explicitIndexes;
    std::vector<IndexInfo> uniqueConstraints;
    int recordCount;
    int nextRecordID;
    TableMeta() : recordCount(0), nextRecordID(1) {}
    int getColumnIndex(const std::string& colName) const {
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i].name == colName) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }
    const ColumnDef* getColumn(const std::string& colName) const {
        for (const auto& col : columns) {
            if (col.name == colName) {
                return &col;
            }
        }
        return nullptr;
    }
    int calculateRecordSize() const {
        int size = 4;
        for (const auto& col : columns) {
            if (col.type == DataType::INT) {
                size += 4;
            } else if (col.type == DataType::FLOAT) {
                size += 4;
            } else if (col.type == DataType::VARCHAR) {
                size += col.length + 4;
            }
        }
        return size;
    }
    bool hasIndex(const std::string& colName) const {
        for (const auto& idx : indexes) {
            if (idx == colName) return true;
        }
        return false;
    }
    bool isPrimaryKey(const std::string& colName) const {
        for (const auto& pk : primaryKey) {
            if (pk == colName) return true;
        }
        for (const auto& pk : primaryKeyColumns) {
            if (pk == colName) return true;
        }
        return false;
    }
};
class SystemManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    std::string baseDir;
    std::string currentDB;
    std::string currentDBPath;

    std::map<std::string, TableMeta> tableMetas;

    std::map<std::string, std::unique_ptr<RecordManager>> tableRecordManagers;
    std::map<std::string, int> tableFileIDs;
    std::unique_ptr<IndexManager> indexManager;
    bool createDirectory(const std::string& path);
    bool removeDirectory(const std::string& path);
    bool saveTableMeta(const std::string& tableName);
    bool loadTableMeta(const std::string& tableName);
    std::string getTableDataPath(const std::string& tableName);
    std::string getTableMetaPath(const std::string& tableName);

public:
    SystemManager(FileManager* fm, BufPageManager* bpm, const std::string& dir = "./data");
    ~SystemManager();
    bool createDatabase(const std::string& dbName);
    bool dropDatabase(const std::string& dbName);
    bool useDatabase(const std::string& dbName);
    std::vector<std::string> showDatabases();
    std::string getCurrentDatabase() const { return currentDB; }
    bool createTable(const std::string& tableName,
                     const std::vector<ColumnDef>& columns,
                     const std::vector<std::string>& primaryKey = std::vector<std::string>(),
                     const std::vector<KeyDef>& foreignKeys = std::vector<KeyDef>());
    bool dropTable(const std::string& tableName);
    std::vector<std::string> showTables();
    TableMeta describeTable(const std::string& tableName);
    bool tableExists(const std::string& tableName);
    TableMeta* getTableMeta(const std::string& tableName);
    bool createIndex(const std::string& tableName, const std::string& columnName,
                     const std::string& indexName = "");
    bool dropIndex(const std::string& tableName, const std::string& indexName);
    std::vector<std::string> showIndexes();
    bool addPrimaryKey(const std::string& tableName, const std::vector<std::string>& columns);
    bool dropPrimaryKey(const std::string& tableName);
    bool addForeignKey(const std::string& tableName, const KeyDef& fk);
    bool dropForeignKey(const std::string& tableName, const std::string& fkName);
    RecordManager* getRecordManager(const std::string& tableName);
    IndexManager* getIndexManager() { return indexManager.get(); }
    BufPageManager* getBufPageManager() { return bufPageManager; }
    int getTableFileID(const std::string& tableName) {
        auto it = tableFileIDs.find(tableName);
        return (it != tableFileIDs.end()) ? it->second : -1;
    }
    int getNextRecordID(const std::string& tableName);
    void updateRecordCount(const std::string& tableName, int delta);
    void closeAllTables();
    void flush();
};

#endif

