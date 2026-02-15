/*
 * simple_example.cpp
 * 
 * 简单的记录管理系统使用示例
 * 演示最基本的增删改查操作
 */

#include "RecordManager.h"
#include "../filesystem/bufmanager/BufPageManager.h"
#include "../filesystem/fileio/FileManager.h"
#include "../filesystem/utils/pagedef.h"
#include <iostream>
#include <cstring>

using namespace std;

int main() {
    // ========== 步骤1: 初始化系统 ==========
    cout << "初始化系统..." << endl;
    MyBitMap::initConst();  // 必须调用！
    
    // ========== 步骤2: 创建管理器 ==========
    FileManager* fm = new FileManager();
    BufPageManager* bpm = new BufPageManager(fm);
    
    // ========== 步骤3: 打开数据库文件 ==========
    const char* dbFile = "simple_db.dat";
    fm->createFile(dbFile);
    int fileID;
    fm->openFile(dbFile, fileID);
    cout << "数据库文件已打开，文件ID: " << fileID << endl;
    
    // ========== 步骤4: 创建记录管理器 ==========
    RecordManager* rm = new RecordManager(fm, bpm, fileID);
    cout << "记录管理器已创建" << endl << endl;
    
    // ========== 步骤5: 插入记录 ==========
    cout << "【插入记录】" << endl;
    
    // 插入整数数组
    unsigned int numbers[] = {10, 20, 30, 40, 50};
    rm->insertRecord(1, numbers, 5);
    cout << "✓ 插入记录ID=1: 整数数组 {10, 20, 30, 40, 50}" << endl;
    
    // 插入字符串
    const char* message = "Hello, Record Management System!";
    rm->insertRecord(2, message, strlen(message) + 1);
    cout << "✓ 插入记录ID=2: 字符串 \"" << message << "\"" << endl;
    
    // 插入更多记录
    for (int i = 3; i <= 5; i++) {
        char buffer[50];
        sprintf(buffer, "Record number %d", i);
        rm->insertRecord(i, buffer, strlen(buffer) + 1);
        cout << "✓ 插入记录ID=" << i << ": \"" << buffer << "\"" << endl;
    }
    
    // ========== 步骤6: 查询记录 ==========
    cout << "\n【查询记录】" << endl;
    
    // 查询整数数组
    unsigned int readNumbers[10];
    int len = rm->getRecord(1, readNumbers, 10);
    if (len > 0) {
        cout << "✓ 查询记录ID=1: ";
        for (int i = 0; i < len; i++) {
            cout << readNumbers[i] << " ";
        }
        cout << endl;
    }
    
    // 查询字符串
    char readMessage[256];
    len = rm->getRecord(2, readMessage, 256);
    if (len > 0) {
        cout << "✓ 查询记录ID=2: \"" << readMessage << "\"" << endl;
    }
    
    // ========== 步骤7: 更新记录 ==========
    cout << "\n【更新记录】" << endl;
    const char* newMessage = "Updated message!";
    if (rm->updateRecord(2, newMessage, strlen(newMessage) + 1)) {
        cout << "✓ 更新记录ID=2成功" << endl;
        
        // 验证更新
        len = rm->getRecord(2, readMessage, 256);
        if (len > 0) {
            cout << "  新内容: \"" << readMessage << "\"" << endl;
        }
    }
    
    // ========== 步骤8: 列出所有记录 ==========
    cout << "\n【列出所有记录】" << endl;
    int recordIDs[100];
    int totalCount = rm->getAllRecordIDs(recordIDs, 100);
    cout << "总记录数: " << totalCount << endl;
    cout << "记录ID列表: ";
    for (int i = 0; i < totalCount; i++) {
        cout << recordIDs[i] << " ";
    }
    cout << endl;
    
    // ========== 步骤9: 删除记录 ==========
    cout << "\n【删除记录】" << endl;
    if (rm->deleteRecord(3)) {
        cout << "✓ 删除记录ID=3成功" << endl;
    }
    
    // 再次列出所有记录
    totalCount = rm->getAllRecordIDs(recordIDs, 100);
    cout << "删除后的总记录数: " << totalCount << endl;
    
    // ========== 步骤10: 统计信息 ==========
    cout << "\n【统计信息】" << endl;
    int totalRecords, totalPages;
    rm->getStatistics(totalRecords, totalPages);
    cout << "总记录数: " << totalRecords << endl;
    cout << "总页数: " << totalPages << endl;
    
    // ========== 步骤11: 关闭系统 ==========
    cout << "\n关闭系统..." << endl;
    rm->close();  // 写回所有脏页
    delete rm;
    delete bpm;
    delete fm;
    
    cout << "\n程序执行完成！" << endl;
    return 0;
}

