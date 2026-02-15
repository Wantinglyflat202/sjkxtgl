#include "RecordManager.h"








#include <unordered_map>

RecordManager::RecordManager(FileManager* fm, BufPageManager* bpm, int fid, bool fixed, int rSize, bool forceInit) {
    fileManager = fm;
    bufPageManager = bpm;
    fileID = fid;
    fixedSize = fixed;
    recordSize = rSize;
    tailPageID = 0;

    int index;
    BufType patchouli = bufPageManager->getPage(fileID, 0, index);


    bool needInit = forceInit;
    if (!needInit) {
        int freeStart = patchouli[PAGE_FREE_START_OFFSET];
        int nextPage = (int)patchouli[PAGE_NEXT_PAGE_OFFSET];
        needInit = (freeStart < PAGE_DATA_START || freeStart > PAGE_INT_NUM ||
                     (nextPage != (int)-1 && (nextPage < 0 || nextPage > 1000000)));
    }
    if (needInit) {
        patchouli[PAGE_TYPE_OFFSET] = 0;
        patchouli[PAGE_RECORD_COUNT_OFFSET] = 0;
        patchouli[PAGE_FREE_START_OFFSET] = PAGE_DATA_START;
        patchouli[PAGE_NEXT_PAGE_OFFSET] = (unsigned int)-1;
        bufPageManager->markDirty(index);
        tailPageID = 0;
    } else {

        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        while (nextPage != -1 && nextPage < 1000000) {
            tailPageID = nextPage;
            patchouli = bufPageManager->getPage(fileID, tailPageID, index);
            getPageHeader(patchouli, recordCount, freeStart, nextPage);
        }
    }
    bufPageManager->access(index);
}
void RecordManager::getPageHeader(BufType patchouli, int& recordCount, int& freeStart, int& nextPage) {
    recordCount = patchouli[PAGE_RECORD_COUNT_OFFSET];
    freeStart = patchouli[PAGE_FREE_START_OFFSET];
    nextPage = patchouli[PAGE_NEXT_PAGE_OFFSET];
}
void RecordManager::setPageHeader(BufType patchouli, int recordCount, int freeStart, int nextPage) {
    patchouli[PAGE_RECORD_COUNT_OFFSET] = recordCount;
    patchouli[PAGE_FREE_START_OFFSET] = freeStart;
    patchouli[PAGE_NEXT_PAGE_OFFSET] = nextPage;
}
int RecordManager::findRecordInPage(BufType patchouli, int recordID, int& offset) {
    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    int pos = PAGE_DATA_START;
    while (pos < freeStart) {
        int recordLen = patchouli[pos];
        if (recordLen <= 0 || pos + recordLen > PAGE_INT_NUM) break;
        int rid = patchouli[pos + 1];
        if (rid == recordID && rid != 0) {
            offset = pos;
            return recordLen;
        }
        pos += recordLen;
    }
    return -1;
}
int RecordManager::findFreeSpace(BufType patchouli, int requiredSize) {
    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    if (freeStart + requiredSize > PAGE_INT_NUM) {
        return -1;
    }
    return freeStart;
}
bool RecordManager::insertRecordInPage(BufType patchouli, int recordID, BufType alice, int dataLen, int pageIndex) {
    int offset;
    if (findRecordInPage(patchouli, recordID, offset) > 0) {
        return false;
    }
    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    int totalLen = RECORD_HEADER_SIZE + dataLen;
    int insertPos = findFreeSpace(patchouli, totalLen);
    if (insertPos == -1) {
        return false;
    }
    patchouli[insertPos] = totalLen;
    patchouli[insertPos + 1] = recordID;
    for (int i = 0; i < dataLen; i++) {
        patchouli[insertPos + RECORD_HEADER_SIZE + i] = alice[i];
    }
    int newFreeStart = freeStart + totalLen;
    setPageHeader(patchouli, recordCount + 1, newFreeStart, nextPage);
    bufPageManager->markDirty(pageIndex);
    return true;
}
bool RecordManager::deleteRecordInPage(BufType patchouli, int recordID, int pageIndex) {
    int offset;
    int recordLen = findRecordInPage(patchouli, recordID, offset);
    if (recordLen <= 0) {
        return false;
    }

    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    patchouli[offset + 1] = 0;
    setPageHeader(patchouli, recordCount - 1, freeStart, nextPage);
    bufPageManager->markDirty(pageIndex);
    return true;
}
int RecordManager::getNewPage() {
    int pageID = 0;
    int index;
    BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    while (nextPage != -1) {
        pageID = nextPage;
        patchouli = bufPageManager->getPage(fileID, pageID, index);
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
    }
    if (findFreeSpace(patchouli, RECORD_HEADER_SIZE + 10) != -1) {
        return pageID;
    }
    int newPageID = pageID + 1;
    int newIndex;
    BufType newPage = bufPageManager->allocPage(fileID, newPageID, newIndex, false);
    newPage[PAGE_TYPE_OFFSET] = 0;
    newPage[PAGE_RECORD_COUNT_OFFSET] = 0;
    newPage[PAGE_FREE_START_OFFSET] = PAGE_DATA_START;
    newPage[PAGE_NEXT_PAGE_OFFSET] = -1;
    setPageHeader(patchouli, recordCount, freeStart, newPageID);
    bufPageManager->markDirty(index);
    bufPageManager->markDirty(newIndex);
    return newPageID;
}
bool RecordManager::insertRecord(int recordID, BufType alice, int dataLen) {

    int index;
    BufType patchouli = bufPageManager->getPage(fileID, tailPageID, index);


    if (insertRecordInPage(patchouli, recordID, alice, dataLen, index)) {
        return true;
    }


    int recordCount, freeStart, nextPage;
    getPageHeader(patchouli, recordCount, freeStart, nextPage);
    int newPageID = tailPageID + 1;
    int newIndex;
    BufType newPage = bufPageManager->allocPage(fileID, newPageID, newIndex, false);
    newPage[PAGE_TYPE_OFFSET] = 0;
    newPage[PAGE_RECORD_COUNT_OFFSET] = 0;
    newPage[PAGE_FREE_START_OFFSET] = PAGE_DATA_START;
    newPage[PAGE_NEXT_PAGE_OFFSET] = -1;
    setPageHeader(patchouli, recordCount, freeStart, newPageID);
    bufPageManager->markDirty(index);
    bufPageManager->markDirty(newIndex);


    tailPageID = newPageID;


    return insertRecordInPage(newPage, recordID, alice, dataLen, newIndex);
}
bool RecordManager::insertRecord(int recordID, const char* alice, int dataLen) {
    int intLen = (dataLen + 3) / 4;
    BufType intData = new unsigned int[intLen];
    memset(intData, 0, intLen * 4);
    memcpy(intData, alice, dataLen);
    bool youmu = insertRecord(recordID, intData, intLen);
    delete[] intData;
    return youmu;
}
bool RecordManager::deleteRecord(int recordID) {
    int pageID = 0;
    while (true) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        if (deleteRecordInPage(patchouli, recordID, index)) {
            return true;
        }
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }
    return false;
}

bool RecordManager::updateRecord(int recordID, BufType newData, int dataLen) {
    if (!deleteRecord(recordID)) {
        return false;
    }
    return insertRecord(recordID, newData, dataLen);
}
bool RecordManager::updateRecord(int recordID, const char* newData, int dataLen) {
    int intLen = (dataLen + 3) / 4;
    BufType intData = new unsigned int[intLen];
    memset(intData, 0, intLen * 4);
    memcpy(intData, newData, dataLen);
    bool youmu = updateRecord(recordID, intData, intLen);
    delete[] intData;
    return youmu;
}
int RecordManager::getRecord(int recordID, BufType alice, int maxLen) {
    int pageID = 0;
    while (true) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        int offset;
        int recordLen = findRecordInPage(patchouli, recordID, offset);
        if (recordLen > 0) {
            int dataLen = recordLen - RECORD_HEADER_SIZE;
            int copyLen = (dataLen < maxLen) ? dataLen : maxLen;
            for (int i = 0; i < copyLen; i++) {
                alice[i] = patchouli[offset + RECORD_HEADER_SIZE + i];
            }
            bufPageManager->access(index);
            return copyLen;
        }
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }
    return -1;
}
int RecordManager::getRecord(int recordID, char* alice, int maxLen) {
    int intMaxLen = (maxLen + 3) / 4;
    BufType intData = new unsigned int[intMaxLen];
    int youmu = getRecord(recordID, intData, intMaxLen);
    if (youmu > 0) {
        int byteLen = youmu * 4;
        if (byteLen > maxLen) byteLen = maxLen;
        memcpy(alice, intData, byteLen);
        delete[] intData;
        return byteLen;
    }
    delete[] intData;
    return -1;
}
bool RecordManager::recordExists(int recordID) {
    int pageID = 0;
    while (true) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        int offset;
        if (findRecordInPage(patchouli, recordID, offset) > 0) {
            bufPageManager->access(index);
            return true;
        }
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }
    return false;
}
int RecordManager::getAllRecordIDs(int* recordIDs, int maxCount) {
    int count = 0;
    int pageID = 0;
    while (count < maxCount) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        int pos = PAGE_DATA_START;
        while (pos < freeStart && count < maxCount) {
            int recordLen = patchouli[pos];
            if (recordLen <= 0 || pos + recordLen > PAGE_INT_NUM) {
                break;
            }
            int rid = patchouli[pos + 1];
            if (rid != 0) {
                recordIDs[count++] = rid;
            }
            pos += recordLen;
        }
        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }
    return count;
}
void RecordManager::getStatistics(int& totalRecords, int& totalPages) {
    totalRecords = 0;
    totalPages = 0;
    int pageID = 0;
    while (true) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);
        totalRecords += recordCount;
        totalPages++;
        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }
}

int RecordManager::getAllRecordsDirect(std::vector<int>& recordIDs,
                                        std::vector<std::vector<char>>& records) {
    recordIDs.clear();
    records.clear();

    int pageID = 0;
    int count = 0;

    while (true) {
        int index;
        BufType patchouli = bufPageManager->getPage(fileID, pageID, index);
        int recordCount, freeStart, nextPage;
        getPageHeader(patchouli, recordCount, freeStart, nextPage);


        int pos = PAGE_DATA_START;
        while (pos < freeStart) {
            int recordLen = patchouli[pos];
            if (recordLen <= 0 || pos + recordLen > PAGE_INT_NUM) {
                break;
            }
            int rid = patchouli[pos + 1];
            if (rid != 0) {
                recordIDs.push_back(rid);


                int dataLen = recordLen - RECORD_HEADER_SIZE;
                std::vector<char> alice(dataLen * 4);
                memcpy(alice.data(), &patchouli[pos + RECORD_HEADER_SIZE], dataLen * 4);
                records.push_back(std::move(alice));
                count++;
            }
            pos += recordLen;
        }

        bufPageManager->access(index);

        if (nextPage == -1) {
            break;
        }
        pageID = nextPage;
    }

    return count;
}
void RecordManager::close() {
    bufPageManager->close();
}

