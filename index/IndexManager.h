#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include "BPlusTree.h"
#include <string>
#include <map>
#include <memory>
class IndexManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    std::string basePath; 
    
    std::map<std::string, std::unique_ptr<BPlusTree>> openIndexes;
    std::map<std::string, int> indexFileIDs;
    std::string getIndexPath(const std::string& tableName, const std::string& columnName);
    std::string getIndexKey(const std::string& tableName, const std::string& columnName);
    
public:
    IndexManager(FileManager* fm, BufPageManager* bpm, const std::string& path);
    
    ~IndexManager();
    bool createIndex(const std::string& tableName, const std::string& columnName,
                     KeyType keyType, int keyLength = 0);
    bool dropIndex(const std::string& tableName, const std::string& columnName);
    bool indexExists(const std::string& tableName, const std::string& columnName);
    BPlusTree* openIndex(const std::string& tableName, const std::string& columnName);
    void closeIndex(const std::string& tableName, const std::string& columnName);
    void closeAll();
    bool insertEntry(const std::string& tableName, const std::string& columnName,
                     int key, const RID& rid);
    bool insertEntry(const std::string& tableName, const std::string& columnName,
                     double key, const RID& rid);
    bool insertEntry(const std::string& tableName, const std::string& columnName,
                     const std::string& key, const RID& rid);
    bool deleteEntry(const std::string& tableName, const std::string& columnName, int key);
    bool deleteEntry(const std::string& tableName, const std::string& columnName, double key);
    bool deleteEntry(const std::string& tableName, const std::string& columnName,
                     const std::string& key);
    bool searchEntry(const std::string& tableName, const std::string& columnName,
                     int key, RID& rid);
    bool searchEntry(const std::string& tableName, const std::string& columnName,
                     double key, RID& rid);

    bool searchEntry(const std::string& tableName, const std::string& columnName,
                     const std::string& key, RID& rid);
    std::vector<RID> rangeSearch(const std::string& tableName, const std::string& columnName,
                                  int lowKey, int highKey, bool includeLow = true, bool includeHigh = true);
    std::vector<RID> rangeSearch(const std::string& tableName, const std::string& columnName,
                                  double lowKey, double highKey, bool includeLow = true, bool includeHigh = true);
    std::vector<RID> rangeSearch(const std::string& tableName, const std::string& columnName,
                                  const std::string& lowKey, const std::string& highKey,
                                  bool includeLow = true, bool includeHigh = true);
    void setBasePath(const std::string& path) { basePath = path; }
};

#endif

