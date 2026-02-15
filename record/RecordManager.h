#ifndef RECORD_MANAGER
#define RECORD_MANAGER
#include "../filesystem/bufmanager/BufPageManager.h"
#include "../filesystem/fileio/FileManager.h"
#include "../filesystem/utils/pagedef.h"
#include <cstring>
#include <iostream>
#include <vector>
using namespace std;

#define PAGE_HEADER_SIZE 16
#define PAGE_DATA_START PAGE_HEADER_SIZE
#define MAX_RECORD_SIZE (PAGE_INT_NUM - PAGE_HEADER_SIZE - 10)
#define PAGE_TYPE_OFFSET 0
#define PAGE_RECORD_COUNT_OFFSET 1
#define PAGE_FREE_START_OFFSET 2
#define PAGE_NEXT_PAGE_OFFSET 3
#define RECORD_HEADER_SIZE 2
class RecordManager {
private:
    FileManager* fileManager;
    BufPageManager* bufPageManager;
    int fileID;
    int recordSize;
    bool fixedSize;
    int tailPageID;
    void getPageHeader(BufType page, int& recordCount, int& freeStart, int& nextPage);
    void setPageHeader(BufType page, int recordCount, int freeStart, int nextPage);
    int findRecordInPage(BufType page, int recordID, int& offset);
    bool insertRecordInPage(BufType page, int recordID, BufType data, int dataLen, int pageIndex);
    bool deleteRecordInPage(BufType page, int recordID, int pageIndex);
    int getNewPage();
    int findFreeSpace(BufType page, int requiredSize);
    void compactPage(BufType page, int pageIndex);

public:
    RecordManager(FileManager* fm, BufPageManager* bpm, int fid, bool fixed = false, int rSize = 0, bool forceInit = false);
    bool insertRecord(int recordID, BufType data, int dataLen);
    bool insertRecord(int recordID, const char* data, int dataLen);
    bool deleteRecord(int recordID);
    bool updateRecord(int recordID, BufType newData, int dataLen);
    bool updateRecord(int recordID, const char* newData, int dataLen);
    int getRecord(int recordID, BufType data, int maxLen);
    int getRecord(int recordID, char* data, int maxLen);
    bool recordExists(int recordID);
    int getAllRecordIDs(int* recordIDs, int maxCount);





    int getAllRecordsDirect(std::vector<int>& recordIDs,
                            std::vector<std::vector<char>>& records);
    void close();
    void getStatistics(int& totalRecords, int& totalPages);
};
#endif

