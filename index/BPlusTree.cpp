#include "BPlusTree.h"
#include <cmath>
#include <queue>

BPlusTree::BPlusTree(FileManager* fm, BufPageManager* bpm, int fid, KeyType kType, int kLen)
    : fileManager(fm), bufPageManager(bpm), fileID(fid), keyType(kType), keyLength(kLen),
      rootPage(-1), firstLeaf(-1) {
    order = calculateOrder();
}
int BPlusTree::calculateOrder() {
    int availableInts = PAGE_INT_NUM - BP_HEADER_SIZE;
    
    int keySize = 1; 
    if (keyType == KeyType::VARCHAR) {
        keySize = (keyLength + 3) / 4 + 1; 
    }
    int leafEntrySize = keySize + 2;
    int internalEntrySize = keySize + 1;
    int leafOrder = availableInts / leafEntrySize;
    int internalOrder = (availableInts - 1) / internalEntrySize; 
    return std::min(leafOrder, internalOrder);
}
bool BPlusTree::initialize() {
    int index;
    BufType headerPage = bufPageManager->allocPage(fileID, 0, index, false);
    
    headerPage[0] = BP_MAGIC;
    headerPage[1] = -1;  // 根节点页号
    headerPage[2] = -1;  // 第一个叶子页号
    headerPage[3] = static_cast<int>(keyType);
    headerPage[4] = keyLength;
    headerPage[5] = 0;   // 节点总数
    headerPage[6] = 0;   // 记录总数
    for (int i = 7; i < BP_HEADER_SIZE; i++) {
        headerPage[i] = 0;
    }
    bufPageManager->markDirty(index);
    rootPage = -1;
    firstLeaf = -1;
    return true;
}
bool BPlusTree::load() {
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    
    if (headerPage[0] != BP_MAGIC) {
        return false;  
    }
    
    rootPage = headerPage[1];
    firstLeaf = headerPage[2];
    keyType = static_cast<KeyType>(headerPage[3]);
    keyLength = headerPage[4];
    order = calculateOrder();
    bufPageManager->access(index);
    return true;
}
void BPlusTree::updateHeader() {
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    
    headerPage[1] = rootPage;
    headerPage[2] = firstLeaf;
    
    bufPageManager->markDirty(index);
}
int BPlusTree::allocateNewPage() {

    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    int nodeCount = headerPage[5];
    int newPageNum = nodeCount + 1;
    headerPage[5] = nodeCount + 1;
    bufPageManager->markDirty(index);
    return newPageNum;
}
BPlusTreeNode BPlusTree::readNode(int pageNum) {
    BPlusTreeNode node;
    node.pageNum = pageNum;
    int index;
    BufType page = bufPageManager->getPage(fileID, pageNum, index);
    int pageType = page[0];
    node.isLeaf = (pageType == BP_PAGE_LEAF);
    node.keyCount = page[1];
    node.parent = page[2];
    if (node.isLeaf) {
        node.nextLeaf = page[3];
        node.prevLeaf = page[4];
    }
    int pos = BP_HEADER_SIZE;
    for (int i = 0; i < node.keyCount; i++) {
        if (keyType == KeyType::INT) {
            node.keys.push_back(static_cast<int>(page[pos++]));
        } else if (keyType == KeyType::FLOAT) {
            float f;
            memcpy(&f, &page[pos++], sizeof(float));
            node.floatKeys.push_back(f);
        } else {
            int len = page[pos++];
            char* str = new char[len + 1];
            int intCount = (len + 3) / 4;
            memcpy(str, &page[pos], len);
            str[len] = '\0';
            pos += intCount;
            node.strKeys.push_back(std::string(str));
            delete[] str;
        }
        if (node.isLeaf) {
            RID rid;
            rid.pageNum = page[pos++];
            rid.slotNum = page[pos++];
            node.rids.push_back(rid);
        } else {
            node.children.push_back(page[pos++]);
        }
    }
    if (!node.isLeaf && node.keyCount > 0) {
        node.children.push_back(page[pos]);
    }
    bufPageManager->access(index);
    return node;
}
void BPlusTree::writeNode(const BPlusTreeNode& node) {
    int index;
    BufType page = bufPageManager->getPage(fileID, node.pageNum, index);
    memset(page + BP_HEADER_SIZE, 0, (PAGE_INT_NUM - BP_HEADER_SIZE) * sizeof(unsigned int));
    page[0] = node.isLeaf ? BP_PAGE_LEAF : BP_PAGE_INTERNAL;
    page[1] = node.keyCount;
    page[2] = node.parent;
    if (node.isLeaf) {
        page[3] = node.nextLeaf;
        page[4] = node.prevLeaf;
    }
    int pos = BP_HEADER_SIZE;
    for (int i = 0; i < node.keyCount; i++) {
        if (keyType == KeyType::INT) {
            page[pos++] = static_cast<unsigned int>(node.keys[i]);
        } else if (keyType == KeyType::FLOAT) {
            memcpy(&page[pos++], &node.floatKeys[i], sizeof(float));
        } else {
            const std::string& str = node.strKeys[i];
            int len = str.length();
            page[pos++] = len;
            int intCount = (len + 3) / 4;
            memcpy(&page[pos], str.c_str(), len);
            pos += intCount;
        }
        if (node.isLeaf) {
            page[pos++] = node.rids[i].pageNum;
            page[pos++] = node.rids[i].slotNum;
        } else {
            page[pos++] = node.children[i];
        }
    }
    if (!node.isLeaf && !node.children.empty()) {
        page[pos] = node.children.back();
    }
    bufPageManager->markDirty(index);
}
int BPlusTree::compareKeys(int key1, int key2) {
    if (key1 < key2) return -1;
    if (key1 > key2) return 1;
    return 0;
}
int BPlusTree::findLeaf(int key) {
    if (rootPage == -1) return -1;
    int currentPage = rootPage;
    BPlusTreeNode node = readNode(currentPage);
    while (!node.isLeaf) {
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) {
            i++;
        }
        currentPage = node.children[i];
        node = readNode(currentPage);
    }
    return currentPage;
}
void BPlusTree::insertIntoLeaf(BPlusTreeNode& leaf, int key, const RID& rid) {
    int i = 0;
    while (i < leaf.keyCount && leaf.keys[i] < key) {
        i++;
    }
    leaf.keys.insert(leaf.keys.begin() + i, key);
    leaf.rids.insert(leaf.rids.begin() + i, rid);
    leaf.keyCount++;
}
void BPlusTree::insertIntoParent(BPlusTreeNode& left, int key, BPlusTreeNode& right) {
    if (left.parent == -1) {
        int newRootPage = allocateNewPage();       
        BPlusTreeNode newRoot;
        newRoot.pageNum = newRootPage;
        newRoot.isLeaf = false;
        newRoot.keyCount = 1;
        newRoot.parent = -1;
        newRoot.keys.push_back(key);
        newRoot.children.push_back(left.pageNum);
        newRoot.children.push_back(right.pageNum);     
        writeNode(newRoot); 
        left.parent = newRootPage;
        right.parent = newRootPage;
        writeNode(left);
        writeNode(right);
        rootPage = newRootPage;
        updateHeader();
        return;
    }
    BPlusTreeNode parent = readNode(left.parent);
    int i = 0;
    while (i < (int)parent.children.size() && parent.children[i] != left.pageNum) {
        i++;
    }
    // 插入新键和子指针
    parent.keys.insert(parent.keys.begin() + i, key);
    parent.children.insert(parent.children.begin() + i + 1, right.pageNum);
    parent.keyCount++;
    right.parent = parent.pageNum;
    writeNode(right);
    // 检查是否需要分裂
    if (parent.keyCount >= order) {
        splitInternal(parent);
    } else {
        writeNode(parent);
    }
}
void BPlusTree::splitLeaf(BPlusTreeNode& leaf) {
    int mid = leaf.keyCount / 2;
    int newPageNum = allocateNewPage();
    BPlusTreeNode newLeaf;
    newLeaf.pageNum = newPageNum;
    newLeaf.isLeaf = true;
    newLeaf.parent = leaf.parent;
    newLeaf.nextLeaf = leaf.nextLeaf;
    newLeaf.prevLeaf = leaf.pageNum;
    for (int i = mid; i < leaf.keyCount; i++) {
        newLeaf.keys.push_back(leaf.keys[i]);
        newLeaf.rids.push_back(leaf.rids[i]);
        newLeaf.keyCount++;
    }
    leaf.keys.resize(mid);
    leaf.rids.resize(mid);
    leaf.keyCount = mid;
    leaf.nextLeaf = newPageNum;
    if (newLeaf.nextLeaf != -1) {
        BPlusTreeNode nextNode = readNode(newLeaf.nextLeaf);
        nextNode.prevLeaf = newPageNum;
        writeNode(nextNode);
    }
    writeNode(leaf);
    writeNode(newLeaf);
    int newKey = newLeaf.keys[0];
    insertIntoParent(leaf, newKey, newLeaf);
}
void BPlusTree::splitInternal(BPlusTreeNode& node) {
    int mid = node.keyCount / 2;
    int midKey = node.keys[mid];
    int newPageNum = allocateNewPage();
    BPlusTreeNode newNode;
    newNode.pageNum = newPageNum;
    newNode.isLeaf = false;
    newNode.parent = node.parent;
    for (int i = mid + 1; i < node.keyCount; i++) {
        newNode.keys.push_back(node.keys[i]);
        newNode.children.push_back(node.children[i]);
        newNode.keyCount++;
    }
    newNode.children.push_back(node.children.back());
    for (int childPage : newNode.children) {
        BPlusTreeNode child = readNode(childPage);
        child.parent = newPageNum;
        writeNode(child);
    }
    node.keys.resize(mid);
    node.children.resize(mid + 1);
    node.keyCount = mid;
    writeNode(node);
    writeNode(newNode);
    insertIntoParent(node, midKey, newNode);
}
bool BPlusTree::insert(int key, const RID& rid) {
    if (keyType != KeyType::INT) return false;
    if (rootPage == -1) {
        int newPageNum = allocateNewPage();
        BPlusTreeNode leaf;
        leaf.pageNum = newPageNum;
        leaf.isLeaf = true;
        leaf.keyCount = 1;
        leaf.parent = -1;
        leaf.nextLeaf = -1;
        leaf.prevLeaf = -1;
        leaf.keys.push_back(key);
        leaf.rids.push_back(rid);   
        writeNode(leaf);
        rootPage = newPageNum;
        firstLeaf = newPageNum;
        updateHeader();
        int index;
        BufType headerPage = bufPageManager->getPage(fileID, 0, index);
        headerPage[6]++;
        bufPageManager->markDirty(index);
        return true;
    }
    int leafPage = findLeaf(key);
    BPlusTreeNode leaf = readNode(leafPage);
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.keys[i] == key) {
            return false;  // 键已存在
        }
    }
    insertIntoLeaf(leaf, key, rid);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]++;
    bufPageManager->markDirty(index);
    if (leaf.keyCount >= order) {
        splitLeaf(leaf);
    } else {
        writeNode(leaf);
    }
    return true;
}
bool BPlusTree::search(int key, RID& rid) {
    if (keyType != KeyType::INT || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.keys[i] == key) {
            rid = leaf.rids[i];
            return true;
        }
    }
    return false;
}
std::vector<RID> BPlusTree::rangeSearch(int lowKey, int highKey, bool includeLow, bool includeHigh) {
    std::vector<RID> result;
    if (keyType != KeyType::INT || rootPage == -1) return result;
    int leafPage = findLeaf(lowKey);
    if (leafPage == -1) return result;
    BPlusTreeNode leaf = readNode(leafPage);
    bool done = false;
    while (!done && leaf.pageNum != -1) {
        for (int i = 0; i < leaf.keyCount; i++) {
            int k = leaf.keys[i];
            bool aboveLow = includeLow ? (k >= lowKey) : (k > lowKey);
            bool belowHigh = includeHigh ? (k <= highKey) : (k < highKey);
            if (!belowHigh) {
                done = true;
                break;
            }
            if (aboveLow && belowHigh) {
                result.push_back(leaf.rids[i]);
            }
        }
        if (!done && leaf.nextLeaf != -1) {
            leaf = readNode(leaf.nextLeaf);
        } else {
            break;
        }
    }
    return result;
}
void BPlusTree::deleteFromLeaf(BPlusTreeNode& leaf, int key) {
    int i = 0;
    while (i < leaf.keyCount && leaf.keys[i] != key) {
        i++;
    }
    if (i < leaf.keyCount) {
        leaf.keys.erase(leaf.keys.begin() + i);
        leaf.rids.erase(leaf.rids.begin() + i);
        leaf.keyCount--;
    }
}
bool BPlusTree::remove(int key) {
    if (keyType != KeyType::INT || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    bool found = false;
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.keys[i] == key) {
            found = true;
            break;
        }
    }
    if (!found) return false;
    deleteFromLeaf(leaf, key);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]--;
    bufPageManager->markDirty(index);
    if (leaf.keyCount == 0) {
        if (leaf.pageNum == rootPage) {
            rootPage = -1;
            firstLeaf = -1;
            updateHeader();
        } else {
            redistributeOrMerge(leaf);
        }
    } else {
        writeNode(leaf);
        if (leaf.parent != -1) {
        }
    }
    return true;
}
void BPlusTree::redistributeOrMerge(BPlusTreeNode& node) {
    if (node.keyCount > 0) {
        writeNode(node);
    }
}
void BPlusTree::mergeNodes(BPlusTreeNode& left, BPlusTreeNode& right) {
    for (int i = 0; i < right.keyCount; i++) {
        if (keyType == KeyType::INT) {
            left.keys.push_back(right.keys[i]);
        } else if (keyType == KeyType::FLOAT) {
            left.floatKeys.push_back(right.floatKeys[i]);
        } else {
            left.strKeys.push_back(right.strKeys[i]);
        }
        if (left.isLeaf) {
            left.rids.push_back(right.rids[i]);
        } else {
            left.children.push_back(right.children[i]);
        }
        left.keyCount++;
    }
    if (!left.isLeaf && !right.children.empty()) {
        left.children.push_back(right.children.back());
    }
    if (left.isLeaf) {
        left.nextLeaf = right.nextLeaf;
    }
    writeNode(left);
}

void BPlusTree::redistributeNodes(BPlusTreeNode& left, BPlusTreeNode& right, bool leftToRight) {
}
int BPlusTree::compareKeys(float key1, float key2) {
    if (key1 < key2) return -1;
    if (key1 > key2) return 1;
    return 0;
}
int BPlusTree::findLeaf(float key) {
    if (rootPage == -1) return -1;
    int currentPage = rootPage;
    BPlusTreeNode node = readNode(currentPage);
    while (!node.isLeaf) {
        int i = 0;
        while (i < node.keyCount && key >= node.floatKeys[i]) {
            i++;
        }
        currentPage = node.children[i];
        node = readNode(currentPage);
    }
    return currentPage;
}
void BPlusTree::insertIntoLeaf(BPlusTreeNode& leaf, float key, const RID& rid) {
    int i = 0;
    while (i < leaf.keyCount && leaf.floatKeys[i] < key) {
        i++;
    }
    leaf.floatKeys.insert(leaf.floatKeys.begin() + i, key);
    leaf.rids.insert(leaf.rids.begin() + i, rid);
    leaf.keyCount++;
}
void BPlusTree::insertIntoParent(BPlusTreeNode& left, float key, BPlusTreeNode& right) {
    if (left.parent == -1) {
        int newRootPage = allocateNewPage();
        BPlusTreeNode newRoot;
        newRoot.pageNum = newRootPage;
        newRoot.isLeaf = false;
        newRoot.keyCount = 1;
        newRoot.parent = -1;
        newRoot.floatKeys.push_back(key);
        newRoot.children.push_back(left.pageNum);
        newRoot.children.push_back(right.pageNum);
        writeNode(newRoot);
        left.parent = newRootPage;
        right.parent = newRootPage;
        writeNode(left);
        writeNode(right);
        rootPage = newRootPage;
        updateHeader();
        return;
    }
    BPlusTreeNode parent = readNode(left.parent);
    int i = 0;
    while (i < (int)parent.children.size() && parent.children[i] != left.pageNum) {
        i++;
    }
    parent.floatKeys.insert(parent.floatKeys.begin() + i, key);
    parent.children.insert(parent.children.begin() + i + 1, right.pageNum);
    parent.keyCount++;
    right.parent = parent.pageNum;
    writeNode(right);
    if (parent.keyCount >= order) {
        writeNode(parent);
    } else {
        writeNode(parent);
    }
}
bool BPlusTree::insert(float key, const RID& rid) {
    if (keyType != KeyType::FLOAT) return false;
    if (rootPage == -1) {
        int newPageNum = allocateNewPage();
        BPlusTreeNode leaf;
        leaf.pageNum = newPageNum;
        leaf.isLeaf = true;
        leaf.keyCount = 1;
        leaf.parent = -1;
        leaf.nextLeaf = -1;
        leaf.prevLeaf = -1;
        leaf.floatKeys.push_back(key);
        leaf.rids.push_back(rid);
        writeNode(leaf);
        rootPage = newPageNum;
        firstLeaf = newPageNum;
        updateHeader();
        int index;
        BufType headerPage = bufPageManager->getPage(fileID, 0, index);
        headerPage[6]++;
        bufPageManager->markDirty(index);
        return true;
    }
    int leafPage = findLeaf(key);
    BPlusTreeNode leaf = readNode(leafPage);
    insertIntoLeaf(leaf, key, rid);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]++;
    bufPageManager->markDirty(index);
    if (leaf.keyCount >= order) {
        writeNode(leaf);
    } else {
        writeNode(leaf);
    }
    return true;
}

bool BPlusTree::search(float key, RID& rid) {
    if (keyType != KeyType::FLOAT || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.floatKeys[i] == key) {
            rid = leaf.rids[i];
            return true;
        }
    }
    return false;
}
std::vector<RID> BPlusTree::rangeSearch(float lowKey, float highKey, bool includeLow, bool includeHigh) {
    std::vector<RID> result;
    if (keyType != KeyType::FLOAT || rootPage == -1) return result;
    int leafPage = findLeaf(lowKey);
    if (leafPage == -1) return result;
    BPlusTreeNode leaf = readNode(leafPage);
    bool done = false;
    while (!done && leaf.pageNum != -1) {
        for (int i = 0; i < leaf.keyCount; i++) {
            float k = leaf.floatKeys[i];
            bool aboveLow = includeLow ? (k >= lowKey) : (k > lowKey);
            bool belowHigh = includeHigh ? (k <= highKey) : (k < highKey);
            if (!belowHigh) {
                done = true;
                break;
            }
            if (aboveLow && belowHigh) {
                result.push_back(leaf.rids[i]);
            }
        }
        if (!done && leaf.nextLeaf != -1) {
            leaf = readNode(leaf.nextLeaf);
        } else {
            break;
        }
    }
    return result;
}
void BPlusTree::deleteFromLeaf(BPlusTreeNode& leaf, float key) {
    int i = 0;
    while (i < leaf.keyCount && leaf.floatKeys[i] != key) {
        i++;
    }
    if (i < leaf.keyCount) {
        leaf.floatKeys.erase(leaf.floatKeys.begin() + i);
        leaf.rids.erase(leaf.rids.begin() + i);
        leaf.keyCount--;
    }
}
bool BPlusTree::remove(float key) {
    if (keyType != KeyType::FLOAT || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    bool found = false;
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.floatKeys[i] == key) {
            found = true;
            break;
        }
    }
    if (!found) return false;
    deleteFromLeaf(leaf, key);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]--;
    bufPageManager->markDirty(index);
    writeNode(leaf);
    return true;
}
int BPlusTree::compareKeys(const std::string& key1, const std::string& key2) {
    return key1.compare(key2);
}
int BPlusTree::findLeaf(const std::string& key) {
    if (rootPage == -1) return -1;
    int currentPage = rootPage;
    BPlusTreeNode node = readNode(currentPage);
    while (!node.isLeaf) {
        int i = 0;
        while (i < node.keyCount && key >= node.strKeys[i]) {
            i++;
        }
        currentPage = node.children[i];
        node = readNode(currentPage);
    }
    return currentPage;
}
void BPlusTree::insertIntoLeaf(BPlusTreeNode& leaf, const std::string& key, const RID& rid) {
    int i = 0;
    while (i < leaf.keyCount && leaf.strKeys[i] < key) {
        i++;
    }
    leaf.strKeys.insert(leaf.strKeys.begin() + i, key);
    leaf.rids.insert(leaf.rids.begin() + i, rid);
    leaf.keyCount++;
}
void BPlusTree::insertIntoParent(BPlusTreeNode& left, const std::string& key, BPlusTreeNode& right) {
    if (left.parent == -1) {
        int newRootPage = allocateNewPage();    
        BPlusTreeNode newRoot;
        newRoot.pageNum = newRootPage;
        newRoot.isLeaf = false;
        newRoot.keyCount = 1;
        newRoot.parent = -1;
        newRoot.strKeys.push_back(key);
        newRoot.children.push_back(left.pageNum);
        newRoot.children.push_back(right.pageNum);
        writeNode(newRoot);
        left.parent = newRootPage;
        right.parent = newRootPage;
        writeNode(left);
        writeNode(right);
        rootPage = newRootPage;
        updateHeader();
        return;
    }
    BPlusTreeNode parent = readNode(left.parent);
    int i = 0;
    while (i < (int)parent.children.size() && parent.children[i] != left.pageNum) {
        i++;
    }
    parent.strKeys.insert(parent.strKeys.begin() + i, key);
    parent.children.insert(parent.children.begin() + i + 1, right.pageNum);
    parent.keyCount++;
    right.parent = parent.pageNum;
    writeNode(right);
    writeNode(parent);
}
bool BPlusTree::insert(const std::string& key, const RID& rid) {
    if (keyType != KeyType::VARCHAR) return false;
    if (rootPage == -1) {
        int newPageNum = allocateNewPage();
        BPlusTreeNode leaf;
        leaf.pageNum = newPageNum;
        leaf.isLeaf = true;
        leaf.keyCount = 1;
        leaf.parent = -1;
        leaf.nextLeaf = -1;
        leaf.prevLeaf = -1;
        leaf.strKeys.push_back(key);
        leaf.rids.push_back(rid);
        writeNode(leaf);
        rootPage = newPageNum;
        firstLeaf = newPageNum;
        updateHeader();
        int index;
        BufType headerPage = bufPageManager->getPage(fileID, 0, index);
        headerPage[6]++;
        bufPageManager->markDirty(index);
        return true;
    }
    int leafPage = findLeaf(key);
    BPlusTreeNode leaf = readNode(leafPage);
    insertIntoLeaf(leaf, key, rid);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]++;
    bufPageManager->markDirty(index);
    writeNode(leaf);
    return true;
}
bool BPlusTree::search(const std::string& key, RID& rid) {
    if (keyType != KeyType::VARCHAR || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.strKeys[i] == key) {
            rid = leaf.rids[i];
            return true;
        }
    }
    return false;
}
std::vector<RID> BPlusTree::rangeSearch(const std::string& lowKey, const std::string& highKey,
                                         bool includeLow, bool includeHigh) {
    std::vector<RID> result;
    if (keyType != KeyType::VARCHAR || rootPage == -1) return result;
    int leafPage = findLeaf(lowKey);
    if (leafPage == -1) return result;
    BPlusTreeNode leaf = readNode(leafPage);
    bool done = false;
    while (!done && leaf.pageNum != -1) {
        for (int i = 0; i < leaf.keyCount; i++) {
            const std::string& k = leaf.strKeys[i];
            bool aboveLow = includeLow ? (k >= lowKey) : (k > lowKey);
            bool belowHigh = includeHigh ? (k <= highKey) : (k < highKey);
            if (!belowHigh) {
                done = true;
                break;
            }
            if (aboveLow && belowHigh) {
                result.push_back(leaf.rids[i]);
            }
        }
        if (!done && leaf.nextLeaf != -1) {
            leaf = readNode(leaf.nextLeaf);
        } else {
            break;
        }
    }
    return result;
}
void BPlusTree::deleteFromLeaf(BPlusTreeNode& leaf, const std::string& key) {
    int i = 0;
    while (i < leaf.keyCount && leaf.strKeys[i] != key) {
        i++;
    }
    if (i < leaf.keyCount) {
        leaf.strKeys.erase(leaf.strKeys.begin() + i);
        leaf.rids.erase(leaf.rids.begin() + i);
        leaf.keyCount--;
    }
}
bool BPlusTree::remove(const std::string& key) {
    if (keyType != KeyType::VARCHAR || rootPage == -1) return false;
    int leafPage = findLeaf(key);
    if (leafPage == -1) return false;
    BPlusTreeNode leaf = readNode(leafPage);
    bool found = false;
    for (int i = 0; i < leaf.keyCount; i++) {
        if (leaf.strKeys[i] == key) {
            found = true;
            break;
        }
    }
    if (!found) return false;
    deleteFromLeaf(leaf, key);
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    headerPage[6]--;
    bufPageManager->markDirty(index);
    writeNode(leaf);
    return true;
}
std::vector<RID> BPlusTree::getAllRIDs() {
    std::vector<RID> result;
    if (firstLeaf == -1) return result;
    int currentPage = firstLeaf;
    while (currentPage != -1) {
        BPlusTreeNode leaf = readNode(currentPage);
        for (int i = 0; i < leaf.keyCount; i++) {
            result.push_back(leaf.rids[i]);
        }
        currentPage = leaf.nextLeaf;
    }
    return result;
}
void BPlusTree::getStatistics(int& nodeCount, int& recordCount, int& height) {
    int index;
    BufType headerPage = bufPageManager->getPage(fileID, 0, index);
    nodeCount = headerPage[5];
    recordCount = headerPage[6];
    height = 0;
    if (rootPage != -1) {
        int currentPage = rootPage;
        BPlusTreeNode node = readNode(currentPage);
        height = 1;
        while (!node.isLeaf && !node.children.empty()) {
            currentPage = node.children[0];
            node = readNode(currentPage);
            height++;
        }
    }
    bufPageManager->access(index);
}
void BPlusTree::close() {
    bufPageManager->close();
}
void BPlusTree::printTree() {
    if (rootPage == -1) {
        std::cout << "Empty B+ Tree" << std::endl;
        return;
    }
    std::cout << "B+ Tree Structure:" << std::endl;
    printNode(rootPage, 0);
}
void BPlusTree::printNode(int pageNum, int level) {
    BPlusTreeNode node = readNode(pageNum);
    std::string indent(level * 2, ' ');
    if (node.isLeaf) {
        std::cout << indent << "Leaf[" << pageNum << "]: ";
        for (int i = 0; i < node.keyCount; i++) {
            if (keyType == KeyType::INT) {
                std::cout << node.keys[i];
            } else if (keyType == KeyType::FLOAT) {
                std::cout << node.floatKeys[i];
            } else {
                std::cout << node.strKeys[i];
            }
            std::cout << "->(" << node.rids[i].pageNum << "," << node.rids[i].slotNum << ")";
            if (i < node.keyCount - 1) std::cout << ", ";
        }
        std::cout << std::endl;
    } else {
        std::cout << indent << "Internal[" << pageNum << "]: ";
        for (int i = 0; i < node.keyCount; i++) {
            if (keyType == KeyType::INT) {
                std::cout << node.keys[i];
            } else if (keyType == KeyType::FLOAT) {
                std::cout << node.floatKeys[i];
            } else {
                std::cout << node.strKeys[i];
            }
            if (i < node.keyCount - 1) std::cout << ", ";
        }
        std::cout << std::endl;
        
        for (int childPage : node.children) {
            printNode(childPage, level + 1);
        }
    }
}
void BPlusTree::updateParentKey(int parentPage, int oldKey, int newKey) {
}
void BPlusTree::updateParentKey(int parentPage, float oldKey, float newKey) {
}
void BPlusTree::updateParentKey(int parentPage, const std::string& oldKey, const std::string& newKey) {
}
