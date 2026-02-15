#include "SystemManager.h"
#include <fstream>
#include <sstream>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <set>

SystemManager::SystemManager(FileManager* fm, BufPageManager* bpm, const std::string& dir)
    : fileManager(fm), bufPageManager(bpm), baseDir(dir) {
    createDirectory(baseDir);
}
SystemManager::~SystemManager() {
    closeAllTables();
}
bool SystemManager::createDirectory(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return mkdir(path.c_str(), 0755) == 0;
    }
    return S_ISDIR(st.st_mode);
}
bool SystemManager::removeDirectory(const std::string& path) {
    DIR* dir = opendir(path.c_str());
    if (dir == nullptr) return false;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        std::string fullPath = path + "/" + entry->d_name;
        struct stat st;
        if (stat(fullPath.c_str(), &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                removeDirectory(fullPath);
            } else {
                unlink(fullPath.c_str());
            }
        }
    }
    closedir(dir);
    return rmdir(path.c_str()) == 0;
}
std::string SystemManager::getTableDataPath(const std::string& tableName) {
    return currentDBPath + "/" + tableName + ".dat";
}
std::string SystemManager::getTableMetaPath(const std::string& tableName) {
    return currentDBPath + "/" + tableName + ".meta";
}
bool SystemManager::saveTableMeta(const std::string& tableName) {
    if (tableMetas.find(tableName) == tableMetas.end()) {
        return false;
    }
    const TableMeta& meta = tableMetas[tableName];
    std::string metaPath = getTableMetaPath(tableName);
    
    std::ofstream file(metaPath);
    if (!file.is_open()) return false;
    file << "TABLE " << meta.tableName << std::endl;
    file << "COLUMNS " << meta.columns.size() << std::endl;
    for (const auto& col : meta.columns) {
        file << col.name << " ";
        if (col.type == DataType::INT) {
            file << "INT";
        } else if (col.type == DataType::FLOAT) {
            file << "FLOAT";
        } else {
            file << "VARCHAR " << col.length;
        }
        file << " " << (col.notNull ? 1 : 0);
        file << " " << (col.hasDefault ? 1 : 0);
        if (col.hasDefault) {
            if (col.defaultValue.isNull) {
                file << " NULL";
            } else if (col.defaultValue.type == Value::Type::INT) {
                file << " INT " << col.defaultValue.intVal;
            } else if (col.defaultValue.type == Value::Type::FLOAT) {
                file << " FLOAT " << col.defaultValue.floatVal;
            } else {
                file << " STRING " << col.defaultValue.strVal;
            }
        }
        file << std::endl;
    }

    file << "PRIMARY_KEY " << meta.primaryKey.size();
    for (const auto& pk : meta.primaryKey) {
        file << " " << pk;
    }
    file << std::endl;

    file << "FOREIGN_KEYS " << meta.foreignKeys.size() << std::endl;
    for (const auto& fk : meta.foreignKeys) {
        const std::string fkName = fk.name.empty() ? "-" : fk.name;
        file << fkName << " " << fk.columns.size();
        for (const auto& col : fk.columns) {
            file << " " << col;
        }
        file << " " << fk.refTable << " " << fk.refColumns.size();
        for (const auto& col : fk.refColumns) {
            file << " " << col;
        }
        file << std::endl;
    }

    file << "INDEXES " << meta.indexes.size();
    for (const auto& idx : meta.indexes) {
        file << " " << idx;
    }
    file << std::endl;
    file << "EXPLICIT_INDEXES " << meta.explicitIndexes.size() << std::endl;
    for (const auto& idx : meta.explicitIndexes) {
        file << idx.name << " " << idx.columns.size();
        for (const auto& col : idx.columns) {
            file << " " << col;
        }
        file << " " << (idx.isExplicit ? 1 : 0) << " " << (idx.isUnique ? 1 : 0);
        file << std::endl;
    }
    file << "PRIMARY_KEY_COLS " << meta.primaryKeyColumns.size();
    for (const auto& pk : meta.primaryKeyColumns) {
        file << " " << pk;
    }
    file << std::endl;

    file << "RECORD_COUNT " << meta.recordCount << std::endl;
    file << "NEXT_RECORD_ID " << meta.nextRecordID << std::endl;
    file.close();
    return true;
}
bool SystemManager::loadTableMeta(const std::string& tableName) {
    std::string metaPath = getTableMetaPath(tableName);
    std::ifstream file(metaPath);
    if (!file.is_open()) return false;
    TableMeta meta;
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string token;
        iss >> token;
        if (token == "TABLE") {
            iss >> meta.tableName;
        } else if (token == "COLUMNS") {
            int count;
            iss >> count;
            for (int i = 0; i < count && std::getline(file, line); i++) {
                std::istringstream colIss(line);
                ColumnDef col;
                std::string typeStr;
                int notNullInt, hasDefaultInt; 
                colIss >> col.name >> typeStr;
                if (typeStr == "INT") {
                    col.type = DataType::INT;
                } else if (typeStr == "FLOAT") {
                    col.type = DataType::FLOAT;
                } else if (typeStr == "VARCHAR") {
                    col.type = DataType::VARCHAR;
                    colIss >> col.length;
                }
                colIss >> notNullInt >> hasDefaultInt;
                col.notNull = (notNullInt != 0);
                col.hasDefault = (hasDefaultInt != 0); 
                if (col.hasDefault) {
                    std::string valType;
                    colIss >> valType;
                    if (valType == "NULL") {
                        col.defaultValue = Value::makeNull();
                    } else if (valType == "INT") {
                        int val;
                        colIss >> val;
                        col.defaultValue = Value(val);
                    } else if (valType == "FLOAT") {
                        float val;
                        colIss >> val;
                        col.defaultValue = Value(val);
                    } else if (valType == "STRING") {
                        std::string val;
                        colIss >> val;
                        col.defaultValue = Value(val);
                    }
                }               
                meta.columns.push_back(col);
            }
        } else if (token == "PRIMARY_KEY") {
            int count;
            iss >> count;
            for (int i = 0; i < count; i++) {
                std::string pk;
                iss >> pk;
                meta.primaryKey.push_back(pk);
            }
        } else if (token == "FOREIGN_KEYS") {
            int count;
            iss >> count;
            for (int i = 0; i < count && std::getline(file, line); i++) {
                std::istringstream fkIss(line);
                KeyDef fk;
                int colCount, refColCount;
                fkIss >> fk.name >> colCount;
                if (fk.name == "-") {
                    fk.name.clear();
                }
                for (int j = 0; j < colCount; j++) {
                    std::string col;
                    fkIss >> col;
                    fk.columns.push_back(col);
                }
                fkIss >> fk.refTable >> refColCount;
                for (int j = 0; j < refColCount; j++) {
                    std::string col;
                    fkIss >> col;
                    fk.refColumns.push_back(col);
                }
                meta.foreignKeys.push_back(fk);
            }
        } else if (token == "INDEXES") {
            int count;
            iss >> count;
            for (int i = 0; i < count; i++) {
                std::string idx;
                iss >> idx;
                meta.indexes.push_back(idx);
            }
        } else if (token == "EXPLICIT_INDEXES") {
            int count;
            iss >> count;
            for (int i = 0; i < count && std::getline(file, line); i++) {
                std::istringstream idxIss(line);
                IndexInfo idx;
                int colCount, isExplicitInt, isUniqueInt;
                
                idxIss >> idx.name >> colCount;
                for (int j = 0; j < colCount; j++) {
                    std::string col;
                    idxIss >> col;
                    idx.columns.push_back(col);
                }
                idxIss >> isExplicitInt >> isUniqueInt;
                idx.isExplicit = (isExplicitInt != 0);
                idx.isUnique = (isUniqueInt != 0);
                
                meta.explicitIndexes.push_back(idx);
            }
        } else if (token == "PRIMARY_KEY_COLS") {
            int count;
            iss >> count;
            for (int i = 0; i < count; i++) {
                std::string pk;
                iss >> pk;
                meta.primaryKeyColumns.push_back(pk);
            }
        } else if (token == "RECORD_COUNT") {
            iss >> meta.recordCount;
        } else if (token == "NEXT_RECORD_ID") {
            iss >> meta.nextRecordID;
        }
    }
    
    if (meta.primaryKeyColumns.empty() && !meta.primaryKey.empty()) {
        meta.primaryKeyColumns = meta.primaryKey;
    }
    
    file.close();
    tableMetas[tableName] = meta;
    return true;
}

bool SystemManager::createDatabase(const std::string& dbName) {
    std::string dbPath = baseDir + "/" + dbName;
    struct stat st;
    if (stat(dbPath.c_str(), &st) == 0) {
        return false;  // 已存在
    }
    return createDirectory(dbPath);
}

bool SystemManager::dropDatabase(const std::string& dbName) {
    // 不能删除当前使用的数据库
    if (dbName == currentDB) {
        closeAllTables();
        currentDB = "";
        currentDBPath = "";
    }
    std::string dbPath = baseDir + "/" + dbName;
    return removeDirectory(dbPath);
}
bool SystemManager::useDatabase(const std::string& dbName) {
    std::string dbPath = baseDir + "/" + dbName;
    struct stat st;
    if (stat(dbPath.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        return false;
    }
    closeAllTables();
    currentDB = dbName;
    currentDBPath = dbPath;
    indexManager = std::make_unique<IndexManager>(fileManager, bufPageManager, currentDBPath);
    tableMetas.clear();
    DIR* dir = opendir(dbPath.c_str());
    if (dir != nullptr) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name = entry->d_name;
            if (name.length() > 5 && name.substr(name.length() - 5) == ".meta") {
                std::string tableName = name.substr(0, name.length() - 5);
                loadTableMeta(tableName);
            }
        }
        closedir(dir);
    }
    return true;
}
std::vector<std::string> SystemManager::showDatabases() {
    std::vector<std::string> databases;
    DIR* dir = opendir(baseDir.c_str());
    if (dir == nullptr) return databases;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;
        std::string path = baseDir + "/" + entry->d_name;
        struct stat st;
        if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
            databases.push_back(entry->d_name);
        }
    }
    closedir(dir);
    return databases;
}
bool SystemManager::createTable(const std::string& tableName,
                                 const std::vector<ColumnDef>& columns,
                                 const std::vector<std::string>& primaryKey,
                                 const std::vector<KeyDef>& foreignKeys) {
    if (currentDB.empty()) {
        return false; 
    }
    if (tableExists(tableName)) {
        return false;  // 表已存在
    }
    std::set<std::string> columnNames;
    for (const auto& col : columns) {
        if (columnNames.find(col.name) != columnNames.end()) {
            return false;  // 列名重复
        }
        columnNames.insert(col.name);
    }
    TableMeta meta;
    meta.tableName = tableName;
    meta.columns = columns;
    meta.primaryKey = primaryKey;
    meta.primaryKeyColumns = primaryKey;
    meta.foreignKeys = foreignKeys;
    meta.recordCount = 0;
    meta.nextRecordID = 1;
    
    // 主键列必须是 NOT NULL
    std::set<std::string> pkSet(primaryKey.begin(), primaryKey.end());
    for (auto& col : meta.columns) {
        if (pkSet.find(col.name) != pkSet.end()) {
            col.notNull = true;
        }
    }    
    tableMetas[tableName] = meta;
    if (!saveTableMeta(tableName)) {
        tableMetas.erase(tableName);
        return false;
    }
    std::string dataPath = getTableDataPath(tableName);
    if (!fileManager->createFile(dataPath.c_str())) {
        tableMetas.erase(tableName);
        return false;
    }
    
    // 立即初始化数据文件的页面 0，避免缓存中的旧数据残留
    {
        int fileID;
        if (fileManager->openFile(dataPath.c_str(), fileID)) {
            // 使用 forceInit=true 强制初始化页面 0
            auto rm = std::make_unique<RecordManager>(fileManager, bufPageManager, fileID, false, 0, true);
            tableFileIDs[tableName] = fileID;
            tableRecordManagers[tableName] = std::move(rm);
        }
    }
    
    for (const auto& pkCol : primaryKey) {
        const ColumnDef* col = meta.getColumn(pkCol);
        if (col && !meta.hasIndex(pkCol)) {
            KeyType keyType;
            int keyLength = 0;
            if (col->type == DataType::INT) {
                keyType = KeyType::INT;
            } else if (col->type == DataType::FLOAT) {
                keyType = KeyType::FLOAT;
            } else {
                keyType = KeyType::VARCHAR;
                keyLength = col->length;
            }
            indexManager->createIndex(tableName, pkCol, keyType, keyLength);
            tableMetas[tableName].indexes.push_back(pkCol);
        }
    }
    saveTableMeta(tableName);
    return true;
}
bool SystemManager::dropTable(const std::string& tableName) {
    if (currentDB.empty() || !tableExists(tableName)) {
        return false;
    }
    if (tableRecordManagers.find(tableName) != tableRecordManagers.end()) {
        tableRecordManagers.erase(tableName);
        if (tableFileIDs.find(tableName) != tableFileIDs.end()) {
            fileManager->closeFile(tableFileIDs[tableName]);
            tableFileIDs.erase(tableName);
        }
    }
    TableMeta& meta = tableMetas[tableName];
    for (const auto& idx : meta.indexes) {
        indexManager->dropIndex(tableName, idx);
    }
    std::string dataPath = getTableDataPath(tableName);
    unlink(dataPath.c_str());
    std::string metaPath = getTableMetaPath(tableName);
    unlink(metaPath.c_str());
    tableMetas.erase(tableName);
    return true;
}
std::vector<std::string> SystemManager::showTables() {
    std::vector<std::string> tables;
    
    for (const auto& pair : tableMetas) {
        tables.push_back(pair.first);
    }
    
    return tables;
}

TableMeta SystemManager::describeTable(const std::string& tableName) {
    if (tableMetas.find(tableName) != tableMetas.end()) {
        return tableMetas[tableName];
    }
    return TableMeta();
}

bool SystemManager::tableExists(const std::string& tableName) {
    return tableMetas.find(tableName) != tableMetas.end();
}

TableMeta* SystemManager::getTableMeta(const std::string& tableName) {
    if (tableMetas.find(tableName) != tableMetas.end()) {
        return &tableMetas[tableName];
    }
    return nullptr;
}
bool SystemManager::createIndex(const std::string& tableName, const std::string& columnName,
                                 const std::string& indexName) {
    if (!tableExists(tableName)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    const ColumnDef* col = meta.getColumn(columnName);
    if (!col) {
        return false;
    }
    if (meta.hasIndex(columnName)) {
        return false;
    }
    //确定类型
    KeyType keyType;
    int keyLength = 0;
    if (col->type == DataType::INT) {
        keyType = KeyType::INT;
    } else if (col->type == DataType::FLOAT) {
        keyType = KeyType::FLOAT;
    } else {
        keyType = KeyType::VARCHAR;
        keyLength = col->length;
    }

    if (!indexManager->createIndex(tableName, columnName, keyType, keyLength)) {
        return false;
    }
    meta.indexes.push_back(columnName);
    bool isImplicit = false;
    for (const auto& pk : meta.primaryKey) {
        if (pk == columnName) {
            isImplicit = true;
            break;
        }
    }
    if (!isImplicit) {
        for (const auto& fk : meta.foreignKeys) {
            for (const auto& fkCol : fk.columns) {
                if (fkCol == columnName) {
                    isImplicit = true;
                    break;
                }
            }
            if (isImplicit) break;
        }
    }
    if (!isImplicit) {
        IndexInfo idxInfo;
        idxInfo.name = indexName.empty() ? tableName + "_" + columnName + "_idx" : indexName;
        idxInfo.columns.push_back(columnName);
        idxInfo.isExplicit = true;
        idxInfo.isUnique = false;
        meta.explicitIndexes.push_back(idxInfo);
    }
    saveTableMeta(tableName);
    return true;
}
bool SystemManager::dropIndex(const std::string& tableName, const std::string& indexName) {
    if (!tableExists(tableName)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    std::string columnToRemove;
    for (auto it = meta.explicitIndexes.begin(); it != meta.explicitIndexes.end(); ++it) {
        if (it->name == indexName) {
            if (!it->columns.empty()) {
                columnToRemove = it->columns[0];
            }
            meta.explicitIndexes.erase(it);
            break;
        }
    }
    if (!columnToRemove.empty()) {
        indexManager->dropIndex(tableName, columnToRemove);
        auto it = std::find(meta.indexes.begin(), meta.indexes.end(), columnToRemove);
        if (it != meta.indexes.end()) {
            meta.indexes.erase(it);
        }
        saveTableMeta(tableName);
        return true;
    }
    auto it = std::find(meta.indexes.begin(), meta.indexes.end(), indexName);
    if (it == meta.indexes.end()) {
        return false;
    }
    if (!indexManager->dropIndex(tableName, indexName)) {
        return false;
    }
    meta.indexes.erase(it);
    for (auto eit = meta.explicitIndexes.begin(); eit != meta.explicitIndexes.end(); ++eit) {
        if (!eit->columns.empty() && eit->columns[0] == indexName) {
            meta.explicitIndexes.erase(eit);
            break;
        }
    }
    saveTableMeta(tableName);
    return true;
}
std::vector<std::string> SystemManager::showIndexes() {
    std::vector<std::string> indexes;
    for (const auto& pair : tableMetas) {
        for (const auto& idx : pair.second.indexes) {
            indexes.push_back(pair.first + "." + idx);
        }
    }
    
    return indexes;
}
bool SystemManager::addPrimaryKey(const std::string& tableName, 
                                   const std::vector<std::string>& columns) {
    if (!tableExists(tableName)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    if (!meta.primaryKey.empty()) {
        return false;
    }
    for (const auto& col : columns) {
        if (!meta.getColumn(col)) {
            return false;
        }
    }
    meta.primaryKey = columns;
    meta.primaryKeyColumns = columns; 
    
    // 创建索引
    for (const auto& col : columns) {
        if (!meta.hasIndex(col)) {
            const ColumnDef* colDef = meta.getColumn(col);
            if (colDef) {
                KeyType keyType;
                int keyLength = 0;
                if (colDef->type == DataType::INT) {
                    keyType = KeyType::INT;
                } else if (colDef->type == DataType::FLOAT) {
                    keyType = KeyType::FLOAT;
                } else {
                    keyType = KeyType::VARCHAR;
                    keyLength = colDef->length;
                }
                indexManager->createIndex(tableName, col, keyType, keyLength);
                meta.indexes.push_back(col);
            }
        }
    }
    
    // 填充索引：将现有记录插入到新创建的索引中
    // 使用 getAllRecordsDirect 来避免内存限制
    RecordManager* rm = getRecordManager(tableName);
    if (rm && indexManager) {
        // 使用 getAllRecordsDirect 批量读取所有记录
        std::vector<int> recordIDs;
        std::vector<std::vector<char>> records;
        int count = rm->getAllRecordsDirect(recordIDs, records);
        
        for (int i = 0; i < count; i++) {
            // 反序列化记录
            std::vector<Value> values;
            const char* buffer = records[i].data();
            int len = records[i].size();
            unsigned int nullBitmap;
            if (len >= 4) {
                memcpy(&nullBitmap, buffer, 4);
                int pos = 4;
                
                for (size_t j = 0; j < meta.columns.size(); j++) {
                    const ColumnDef& col = meta.columns[j];
                    bool isNull = (nullBitmap & (1 << j)) != 0;
                    
                    if (col.type == DataType::INT) {
                        if (pos + 4 <= len) {
                            if (isNull) {
                                values.push_back(Value::makeNull());
                            } else {
                                int v;
                                memcpy(&v, buffer + pos, 4);
                                values.push_back(Value(v));
                            }
                            pos += 4;
                        }
                    } else if (col.type == DataType::FLOAT) {
                        if (pos + 4 <= len) {
                            if (isNull) {
                                values.push_back(Value::makeNull());
                            } else {
                                float v;
                                memcpy(&v, buffer + pos, 4);
                                values.push_back(Value(v));
                            }
                            pos += 4;
                        }
                    } else if (col.type == DataType::VARCHAR) {
                        if (pos + 4 <= len) {
                            int strLen;
                            memcpy(&strLen, buffer + pos, 4);
                            pos += 4;
                            
                            if (isNull) {
                                values.push_back(Value::makeNull());
                            } else {
                                if (pos + strLen <= len) {
                                    std::string str(buffer + pos, strLen);
                                    while (!str.empty() && str.back() == '\0') {
                                        str.pop_back();
                                    }
                                    values.push_back(Value(str));
                                }
                            }
                            pos += col.length;
                        }
                    }
                }
                
                // 将记录插入到索引中
                for (const auto& pkCol : columns) {
                    int colIdx = meta.getColumnIndex(pkCol);
                    if (colIdx >= 0 && colIdx < (int)values.size() && !values[colIdx].isNull) {
                        RID rid(0, recordIDs[i]);
                        if (meta.columns[colIdx].type == DataType::INT) {
                            indexManager->insertEntry(tableName, pkCol, values[colIdx].intVal, rid);
                        } else if (meta.columns[colIdx].type == DataType::FLOAT) {
                            indexManager->insertEntry(tableName, pkCol, values[colIdx].floatVal, rid);
                        } else {
                            indexManager->insertEntry(tableName, pkCol, values[colIdx].strVal, rid);
                        }
                    }
                }
            }
        }
    }
    
    saveTableMeta(tableName);
    return true;
}
bool SystemManager::dropPrimaryKey(const std::string& tableName) {
    if (!tableExists(tableName)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    
    if (meta.primaryKey.empty() && meta.primaryKeyColumns.empty()) {
        return false;
    }
    meta.primaryKey.clear();
    meta.primaryKeyColumns.clear();
    saveTableMeta(tableName);
    return true;
}
bool SystemManager::addForeignKey(const std::string& tableName, const KeyDef& fk) {
    if (!tableExists(tableName) || !tableExists(fk.refTable)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    TableMeta& refMeta = tableMetas[fk.refTable];
    for (const auto& col : fk.columns) {
        if (!meta.getColumn(col)) {
            return false;
        }
    }
    for (const auto& col : fk.refColumns) {
        if (!refMeta.getColumn(col)) {
            return false;
        }
    }
    meta.foreignKeys.push_back(fk);
    saveTableMeta(tableName);
    return true;
}
bool SystemManager::dropForeignKey(const std::string& tableName, const std::string& fkName) {
    if (!tableExists(tableName)) {
        return false;
    }
    TableMeta& meta = tableMetas[tableName];
    
    for (auto it = meta.foreignKeys.begin(); it != meta.foreignKeys.end(); ++it) {
        if (it->name == fkName) {
            meta.foreignKeys.erase(it);
            saveTableMeta(tableName);
            return true;
        }
    }
    
    return false;
}
RecordManager* SystemManager::getRecordManager(const std::string& tableName) {
    if (!tableExists(tableName)) {
        return nullptr;
    }
    if (tableRecordManagers.find(tableName) != tableRecordManagers.end()) {
        return tableRecordManagers[tableName].get();
    }
    std::string dataPath = getTableDataPath(tableName);
    int fileID;
    if (!fileManager->openFile(dataPath.c_str(), fileID)) {
        return nullptr;
    }
    tableFileIDs[tableName] = fileID;
    auto rm = std::make_unique<RecordManager>(fileManager, bufPageManager, fileID);
    RecordManager* ptr = rm.get();
    tableRecordManagers[tableName] = std::move(rm);
    return ptr;
}
int SystemManager::getNextRecordID(const std::string& tableName) {
    if (!tableExists(tableName)) {
        return -1;
    }
    TableMeta& meta = tableMetas[tableName];
    int id = meta.nextRecordID++;
    // 不立即保存，由批量更新机制处理
    return id;
}
void SystemManager::updateRecordCount(const std::string& tableName, int delta) {
    if (tableExists(tableName)) {
        if (delta > 0) {
            tableMetas[tableName].recordCount += delta;
        }
        // delta == 0 时只保存，不更新计数（用于批量保存）
        saveTableMeta(tableName);
    }
}
void SystemManager::closeAllTables() {
    // 必须先刷新脏页，然后再关闭文件
    // 注意：bufPageManager->close() 会将脏页写入到对应的 fileID
    // 所以必须在关闭文件之前调用
    bufPageManager->close();
    
    // 清除 RecordManager 缓存（RecordManager 使用 bufPageManager，不需要额外关闭）
    tableRecordManagers.clear();
    
    // 关闭所有表文件
    for (const auto& pair : tableFileIDs) {
        fileManager->closeFile(pair.second);
    }
    tableFileIDs.clear();
    
    // 关闭索引
    if (indexManager) {
        indexManager->closeAll();
    }
}
void SystemManager::flush() {
    bufPageManager->close();
}

