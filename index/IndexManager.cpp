#include "IndexManager.h"
#include <fstream>
#include <sys/stat.h>

IndexManager::IndexManager(FileManager* fm, BufPageManager* bpm, const std::string& path)
    : fileManager(fm), bufPageManager(bpm), basePath(path) {
}
IndexManager::~IndexManager() {
    closeAll();
}
std::string IndexManager::getIndexPath(const std::string& tableName, const std::string& columnName) {
    return basePath + "/" + tableName + "_" + columnName + ".idx";
}
std::string IndexManager::getIndexKey(const std::string& tableName, const std::string& columnName) {
    return tableName + "_" + columnName;
}
bool IndexManager::createIndex(const std::string& tableName, const std::string& columnName,
                                KeyType keyType, int keyLength) {
    std::string indexPath = getIndexPath(tableName, columnName);
    std::string indexKey = getIndexKey(tableName, columnName);
    if (indexExists(tableName, columnName)) {
        return false;
    }
    if (!fileManager->createFile(indexPath.c_str())) {
        return false;
    }
    int fileID;
    if (!fileManager->openFile(indexPath.c_str(), fileID)) {
        return false;
    }
    auto tree = std::make_unique<BPlusTree>(fileManager, bufPageManager, fileID, keyType, keyLength);
    if (!tree->initialize()) {
        fileManager->closeFile(fileID);
        return false;
    }
    indexFileIDs[indexKey] = fileID;
    openIndexes[indexKey] = std::move(tree);
    return true;
}
bool IndexManager::dropIndex(const std::string& tableName, const std::string& columnName) {
    std::string indexPath = getIndexPath(tableName, columnName);
    std::string indexKey = getIndexKey(tableName, columnName);
    if (openIndexes.find(indexKey) != openIndexes.end()) {
        closeIndex(tableName, columnName);
    }
    return (remove(indexPath.c_str()) == 0);
}
bool IndexManager::indexExists(const std::string& tableName, const std::string& columnName) {
    std::string indexPath = getIndexPath(tableName, columnName);
    struct stat buffer;
    return (stat(indexPath.c_str(), &buffer) == 0);
}
BPlusTree* IndexManager::openIndex(const std::string& tableName, const std::string& columnName) {
    std::string indexKey = getIndexKey(tableName, columnName);
    if (openIndexes.find(indexKey) != openIndexes.end()) {
        return openIndexes[indexKey].get();
    }
    if (!indexExists(tableName, columnName)) {
        return nullptr;
    }
    std::string indexPath = getIndexPath(tableName, columnName);
    int fileID;
    if (!fileManager->openFile(indexPath.c_str(), fileID)) {
        return nullptr;
    }
    auto tree = std::make_unique<BPlusTree>(fileManager, bufPageManager, fileID, KeyType::INT, 0);
    if (!tree->load()) {
        fileManager->closeFile(fileID);
        return nullptr;
    }
    indexFileIDs[indexKey] = fileID;
    BPlusTree* ptr = tree.get();
    openIndexes[indexKey] = std::move(tree);
    return ptr;
}
void IndexManager::closeIndex(const std::string& tableName, const std::string& columnName) {
    std::string indexKey = getIndexKey(tableName, columnName);
    
    if (openIndexes.find(indexKey) != openIndexes.end()) {
        openIndexes.erase(indexKey);
        if (indexFileIDs.find(indexKey) != indexFileIDs.end()) {
            fileManager->closeFile(indexFileIDs[indexKey]);
            indexFileIDs.erase(indexKey);
        }
    }
}
void IndexManager::closeAll() {
    for (auto& pair : openIndexes) {
        pair.second.reset();
    }
    openIndexes.clear();
    for (auto& pair : indexFileIDs) {
        fileManager->closeFile(pair.second);
    }
    indexFileIDs.clear();
}
bool IndexManager::insertEntry(const std::string& tableName, const std::string& columnName,
                                int key, const RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::INT) {
        return false;
    }
    return tree->insert(key, rid);
}

bool IndexManager::insertEntry(const std::string& tableName, const std::string& columnName,
                                double key, const RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::FLOAT) {
        return false;
    }
    float fkey = (float)key;
    return tree->insert(fkey, rid);
}

bool IndexManager::insertEntry(const std::string& tableName, const std::string& columnName,
                                const std::string& key, const RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::VARCHAR) {
        return false;
    }
    return tree->insert(key, rid);
}

bool IndexManager::deleteEntry(const std::string& tableName, const std::string& columnName, int key) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::INT) {
        return false;
    }
    return tree->remove(key);
}

bool IndexManager::deleteEntry(const std::string& tableName, const std::string& columnName, double key) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::FLOAT) {
        return false;
    }
    float fkey = (float)key;
    return tree->remove(fkey);
}

bool IndexManager::deleteEntry(const std::string& tableName, const std::string& columnName,
                                const std::string& key) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::VARCHAR) {
        return false;
    }
    return tree->remove(key);
}

bool IndexManager::searchEntry(const std::string& tableName, const std::string& columnName,
                                int key, RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::INT) {
        return false;
    }
    return tree->search(key, rid);
}

bool IndexManager::searchEntry(const std::string& tableName, const std::string& columnName,
                                double key, RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::FLOAT) {
        return false;
    }
    float fkey = (float)key;
    return tree->search(fkey, rid);
}

bool IndexManager::searchEntry(const std::string& tableName, const std::string& columnName,
                                const std::string& key, RID& rid) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::VARCHAR) {
        return false;
    }
    return tree->search(key, rid);
}

std::vector<RID> IndexManager::rangeSearch(const std::string& tableName, const std::string& columnName,
                                            int lowKey, int highKey, bool includeLow, bool includeHigh) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::INT) {
        return std::vector<RID>();
    }
    return tree->rangeSearch(lowKey, highKey, includeLow, includeHigh);
}

std::vector<RID> IndexManager::rangeSearch(const std::string& tableName, const std::string& columnName,
                                            double lowKey, double highKey, bool includeLow, bool includeHigh) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::FLOAT) {
        return std::vector<RID>();
    }
    float flowKey = (float)lowKey;
    float fhighKey = (float)highKey;
    return tree->rangeSearch(flowKey, fhighKey, includeLow, includeHigh);
}

std::vector<RID> IndexManager::rangeSearch(const std::string& tableName, const std::string& columnName,
                                            const std::string& lowKey, const std::string& highKey,
                                            bool includeLow, bool includeHigh) {
    BPlusTree* tree = openIndex(tableName, columnName);
    if (!tree || tree->getKeyType() != KeyType::VARCHAR) {
        return std::vector<RID>();
    }
    return tree->rangeSearch(lowKey, highKey, includeLow, includeHigh);
}

