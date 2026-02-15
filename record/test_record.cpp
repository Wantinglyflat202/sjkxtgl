/*
 * test_record.cpp
 * 
 * 记录管理系统测试程序
 * 演示如何使用RecordManager进行记录的增删改查操作
 */

#include "RecordManager.h"
#include "../filesystem/bufmanager/BufPageManager.h"
#include "../filesystem/fileio/FileManager.h"
#include "../filesystem/utils/pagedef.h"
#include <iostream>
#include <cstring>

using namespace std;

int main() {
    // 初始化（必须！）
    MyBitMap::initConst();
    
    // 创建文件管理器和缓冲页管理器
    FileManager* fm = new FileManager();
    BufPageManager* bpm = new BufPageManager(fm);
    
    // 创建或打开数据库文件
    const char* dbFileName = "record_db.dat";
    fm->createFile(dbFileName);
    
    int fileID;
    if (!fm->openFile(dbFileName, fileID)) {
        cout << "打开文件失败！" << endl;
        return -1;
    }
    
    cout << "文件ID: " << fileID << endl;
    
    // 创建记录管理器
    RecordManager* rm = new RecordManager(fm, bpm, fileID, false);
    
    cout << "\n========== 记录管理系统测试 ==========" << endl;
    
    // ========== 测试1: 插入记录 ==========
    cout << "\n【测试1】插入记录" << endl;
    
    // 插入整数数组记录
    unsigned int data1[] = {100, 200, 300, 400, 500};
    if (rm->insertRecord(1, data1, 5)) {
        cout << "✓ 成功插入记录ID=1 (整数数组)" << endl;
    } else {
        cout << "✗ 插入记录失败" << endl;
    }
    
    // 插入字符串记录
    const char* str1 = "Hello, Database!";
    if (rm->insertRecord(2, str1, strlen(str1) + 1)) {
        cout << "✓ 成功插入记录ID=2 (字符串: " << str1 << ")" << endl;
    }
    
    const char* str2 = "这是中文测试";
    if (rm->insertRecord(3, str2, strlen(str2) + 1)) {
        cout << "✓ 成功插入记录ID=3 (字符串: " << str2 << ")" << endl;
    }
    
    // 插入更多记录
    for (int i = 4; i <= 10; i++) {
        char buffer[100];
        sprintf(buffer, "Record %d: This is test data for record number %d", i, i);
        if (rm->insertRecord(i, buffer, strlen(buffer) + 1)) {
            cout << "✓ 成功插入记录ID=" << i << endl;
        }
    }
    
    // ========== 测试2: 查询记录 ==========
    cout << "\n【测试2】查询记录" << endl;
    
    // 查询整数数组记录
    unsigned int readData[10];
    int len = rm->getRecord(1, readData, 10);
    if (len > 0) {
        cout << "✓ 查询记录ID=1成功，数据: ";
        for (int i = 0; i < len && i < 10; i++) {
            cout << readData[i] << " ";
        }
        cout << endl;
    }
    
    // 查询字符串记录
    char readStr[256];
    len = rm->getRecord(2, readStr, 256);
    if (len > 0) {
        cout << "✓ 查询记录ID=2成功，内容: " << readStr << endl;
    }
    
    len = rm->getRecord(3, readStr, 256);
    if (len > 0) {
        cout << "✓ 查询记录ID=3成功，内容: " << readStr << endl;
    }
    
    // ========== 测试3: 更新记录 ==========
    cout << "\n【测试3】更新记录" << endl;
    
    const char* newStr = "Updated: Hello, New Database!";
    if (rm->updateRecord(2, newStr, strlen(newStr) + 1)) {
        cout << "✓ 成功更新记录ID=2" << endl;
        
        // 验证更新
        len = rm->getRecord(2, readStr, 256);
        if (len > 0) {
            cout << "  更新后的内容: " << readStr << endl;
        }
    }
    
    // ========== 测试4: 检查记录是否存在 ==========
    cout << "\n【测试4】检查记录是否存在" << endl;
    
    if (rm->recordExists(1)) {
        cout << "✓ 记录ID=1存在" << endl;
    }
    if (rm->recordExists(99)) {
        cout << "✗ 记录ID=99存在（不应该）" << endl;
    } else {
        cout << "✓ 记录ID=99不存在（正确）" << endl;
    }
    
    // ========== 测试5: 获取所有记录ID ==========
    cout << "\n【测试5】获取所有记录ID" << endl;
    
    int recordIDs[100];
    int totalCount = rm->getAllRecordIDs(recordIDs, 100);
    cout << "✓ 总记录数: " << totalCount << endl;
    cout << "  记录ID列表: ";
    for (int i = 0; i < totalCount; i++) {
        cout << recordIDs[i] << " ";
    }
    cout << endl;
    
    // ========== 测试6: 删除记录 ==========
    cout << "\n【测试6】删除记录" << endl;
    
    if (rm->deleteRecord(5)) {
        cout << "✓ 成功删除记录ID=5" << endl;
    }
    
    if (rm->deleteRecord(99)) {
        cout << "✗ 删除不存在的记录（不应该成功）" << endl;
    } else {
        cout << "✓ 删除不存在的记录失败（正确）" << endl;
    }
    
    // 验证删除
    if (rm->recordExists(5)) {
        cout << "✗ 记录ID=5仍然存在（不应该）" << endl;
    } else {
        cout << "✓ 记录ID=5已删除（正确）" << endl;
    }
    
    // 再次获取所有记录ID
    totalCount = rm->getAllRecordIDs(recordIDs, 100);
    cout << "  删除后的总记录数: " << totalCount << endl;
    
    // ========== 测试7: 统计信息 ==========
    cout << "\n【测试7】统计信息" << endl;
    
    int totalRecords, totalPages;
    rm->getStatistics(totalRecords, totalPages);
    cout << "✓ 总记录数: " << totalRecords << endl;
    cout << "✓ 总页数: " << totalPages << endl;
    
    // ========== 测试8: 插入重复ID（应该失败） ==========
    cout << "\n【测试8】尝试插入重复ID" << endl;
    
    unsigned int dupData[] = {999, 888};
    if (rm->insertRecord(1, dupData, 2)) {
        cout << "✗ 插入重复ID成功（不应该）" << endl;
    } else {
        cout << "✓ 插入重复ID失败（正确）" << endl;
    }
    
    // ========== 清理和关闭 ==========
    cout << "\n【清理】关闭记录管理器" << endl;
    rm->close();
    
    delete rm;
    delete bpm;
    delete fm;
    
    cout << "\n========== 测试完成 ==========" << endl;
    
    return 0;
}

