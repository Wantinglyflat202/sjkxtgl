#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "../filesystem/bufmanager/BufPageManager.h"
#include "../filesystem/fileio/FileManager.h"
#include "../filesystem/utils/pagedef.h"
#include <cstring>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#define BP_PAGE_HEADER 0
#define BP_PAGE_INTERNAL 1
#define BP_PAGE_LEAF 2
#define BP_HEADER_SIZE 16
#define BP_MAGIC 0x42505452
enum class KeyType {
    INT = 0,
    FLOAT = 1,
    VARCHAR = 2
};
struct RID {
    int pageNum;
    int slotNum;
    RID() : pageNum(-1), slotNum(-1) {}
    RID(int p, int s) : pageNum(p), slotNum(s) {}
    bool operator==(const RID& other) const {
        return pageNum == other.pageNum && slotNum == other.slotNum;
    }
    bool operator!=(const RID& other) const {
        return !(*this == other);
    }
    bool isValid() const {
        return pageNum >= 0 && slotNum >= 0;
    }
};
class BPlusTreeNode {
public:
    int pageNum;            // 页号
    bool isLeaf;            // 是否为叶子节点
    int keyCount;           // 键数量
    int parent;             // 父节点页号
    int nextLeaf;           // 下一个叶子 (仅叶子节点)
    int prevLeaf;           // 上一个叶子 (仅叶子节点)
    std::vector<int> keys;          // 整数键
    std::vector<float> floatKeys;   // 浮点键
    std::vector<std::string> strKeys; // 字符串键
    std::vector<int> children;      // 子节点页号 (内部节点)
    std::vector<RID> rids;          // 记录ID (叶子节点)
    
    BPlusTreeNode() : pageNum(-1), isLeaf(true), keyCount(0), 
                      parent(-1), nextLeaf(-1), prevLeaf(-1) {}
};
class BPlusTree {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    int fileID;
    KeyType keyType;
    int keyLength;          // VARCHAR的长度
    int order;              // B+树的阶
    int rootPage;           // 根节点页号
    int firstLeaf;          // 第一个叶子页号
    
    int calculateOrder();
    
    // 读写节点
    BPlusTreeNode readNode(int pageNum);
    void writeNode(const BPlusTreeNode& node);
    
    // 分配新页面
    int allocateNewPage();
    
    // 更新头页面
    void updateHeader();
    
    // 比较键
    int compareKeys(int key1, int key2);
    int compareKeys(float key1, float key2);
    int compareKeys(const std::string& key1, const std::string& key2);
    
    // 查找操作
    int findLeaf(int key);
    int findLeaf(float key);
    int findLeaf(const std::string& key);
    
    // 插入操作辅助函数
    void insertIntoLeaf(BPlusTreeNode& leaf, int key, const RID& rid);
    void insertIntoLeaf(BPlusTreeNode& leaf, float key, const RID& rid);
    void insertIntoLeaf(BPlusTreeNode& leaf, const std::string& key, const RID& rid);
    
    void insertIntoParent(BPlusTreeNode& left, int key, BPlusTreeNode& right);
    void insertIntoParent(BPlusTreeNode& left, float key, BPlusTreeNode& right);
    void insertIntoParent(BPlusTreeNode& left, const std::string& key, BPlusTreeNode& right);
    
    void splitLeaf(BPlusTreeNode& leaf);
    void splitInternal(BPlusTreeNode& node);
    
    // 删除操作辅助函数
    void deleteFromLeaf(BPlusTreeNode& leaf, int key);
    void deleteFromLeaf(BPlusTreeNode& leaf, float key);
    void deleteFromLeaf(BPlusTreeNode& leaf, const std::string& key);
    
    void redistributeOrMerge(BPlusTreeNode& node);
    void mergeNodes(BPlusTreeNode& left, BPlusTreeNode& right);
    void redistributeNodes(BPlusTreeNode& left, BPlusTreeNode& right, bool leftToRight);
    
    // 更新父节点中的键
    void updateParentKey(int parentPage, int oldKey, int newKey);
    void updateParentKey(int parentPage, float oldKey, float newKey);
    void updateParentKey(int parentPage, const std::string& oldKey, const std::string& newKey);
    
public:
    BPlusTree(FileManager* fm, BufPageManager* bpm, int fid, KeyType kType, int kLen = 0);
    
    bool initialize();
    bool load();
    
    bool insert(int key, const RID& rid);
    bool remove(int key);
    bool search(int key, RID& rid);
    std::vector<RID> rangeSearch(int lowKey, int highKey, bool includeLow = true, bool includeHigh = true);
    
    bool insert(float key, const RID& rid);
    bool remove(float key);
    bool search(float key, RID& rid);
    std::vector<RID> rangeSearch(float lowKey, float highKey, bool includeLow = true, bool includeHigh = true);
    
    bool insert(const std::string& key, const RID& rid);
    bool remove(const std::string& key);
    bool search(const std::string& key, RID& rid);
    std::vector<RID> rangeSearch(const std::string& lowKey, const std::string& highKey, 
                                  bool includeLow = true, bool includeHigh = true);

    std::vector<RID> getAllRIDs();
    
    void getStatistics(int& nodeCount, int& recordCount, int& height);
    void close();
    KeyType getKeyType() const { return keyType; }
    void printTree();
private:
    void printNode(int pageNum, int level);
};

#endif

