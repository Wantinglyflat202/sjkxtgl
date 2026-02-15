# 记录管理系统 (Record Management System)

## 概述

这是一个基于页式文件系统的记录管理系统，提供了完整的记录增删改查功能。

## 系统架构

### 页式文件系统接口

系统基于以下两个核心组件：

1. **FileManager** - 文件管理器
   - `createFile(name)` - 创建文件
   - `openFile(name, fileID)` - 打开文件，返回fileID
   - `closeFile(fileID)` - 关闭文件
   - `readPage(fileID, pageID, buf, off)` - 读取页面
   - `writePage(fileID, pageID, buf, off)` - 写入页面

2. **BufPageManager** - 缓冲页管理器
   - `allocPage(fileID, pageID, index, ifRead)` - 分配新页面
   - `getPage(fileID, pageID, index)` - 获取页面（可能在缓存中）
   - `markDirty(index)` - 标记脏页
   - `access(index)` - 标记访问
   - `writeBack(index)` - 写回单个页面
   - `close()` - 关闭所有页面并写回

### 页面布局设计

每个页面大小为 8192 字节（2048个整数），布局如下：

```
页面头部（16个整数 = 64字节）:
  [0]: 页面类型标识 (0=数据页)
  [1]: 记录数量
  [2]: 空闲空间起始位置
  [3]: 下一个页面ID（链表结构，-1表示无）
  [4-15]: 保留字段

数据区（从第16个整数开始）:
  存储可变长度的记录
```

### 记录格式

每条记录格式：
```
[0]: 记录长度（单位：整数，包括长度字段本身）
[1]: 记录ID（唯一标识）
[2...]: 实际数据（可变长度）
```

## RecordManager 接口说明

### 构造函数

```cpp
RecordManager(FileManager* fm, BufPageManager* bpm, int fid, bool fixed = false, int rSize = 0)
```

- `fm`: 文件管理器指针
- `bpm`: 缓冲页管理器指针
- `fid`: 文件ID
- `fixed`: 是否使用固定长度记录（暂未实现）
- `rSize`: 固定记录大小

### 插入记录

```cpp
// 使用整数数组
bool insertRecord(int recordID, BufType data, int dataLen);

// 使用字节数组（字符串等）
bool insertRecord(int recordID, const char* data, int dataLen);
```

### 删除记录

```cpp
bool deleteRecord(int recordID);
```

### 更新记录

```cpp
// 使用整数数组
bool updateRecord(int recordID, BufType newData, int dataLen);

// 使用字节数组
bool updateRecord(int recordID, const char* newData, int dataLen);
```

### 查询记录

```cpp
// 返回整数数组
int getRecord(int recordID, BufType data, int maxLen);

// 返回字节数组
int getRecord(int recordID, char* data, int maxLen);
```

### 其他功能

```cpp
// 检查记录是否存在
bool recordExists(int recordID);

// 获取所有记录ID
int getAllRecordIDs(int* recordIDs, int maxCount);

// 获取统计信息
void getStatistics(int& totalRecords, int& totalPages);

// 关闭记录管理器（写回所有脏页）
void close();
```

## 使用示例

```cpp
#include "RecordManager.h"
#include "filesystem/bufmanager/BufPageManager.h"
#include "filesystem/fileio/FileManager.h"
#include "filesystem/utils/pagedef.h"

int main() {
    // 1. 初始化（必须！）
    MyBitMap::initConst();
    
    // 2. 创建文件管理器和缓冲页管理器
    FileManager* fm = new FileManager();
    BufPageManager* bpm = new BufPageManager(fm);
    
    // 3. 创建或打开数据库文件
    fm->createFile("mydb.dat");
    int fileID;
    fm->openFile("mydb.dat", fileID);
    
    // 4. 创建记录管理器
    RecordManager* rm = new RecordManager(fm, bpm, fileID);
    
    // 5. 插入记录
    unsigned int data[] = {100, 200, 300};
    rm->insertRecord(1, data, 3);
    
    const char* str = "Hello, World!";
    rm->insertRecord(2, str, strlen(str) + 1);
    
    // 6. 查询记录
    unsigned int readData[10];
    int len = rm->getRecord(1, readData, 10);
    
    char readStr[256];
    len = rm->getRecord(2, readStr, 256);
    cout << readStr << endl;
    
    // 7. 更新记录
    const char* newStr = "Updated!";
    rm->updateRecord(2, newStr, strlen(newStr) + 1);
    
    // 8. 删除记录
    rm->deleteRecord(1);
    
    // 9. 关闭
    rm->close();
    delete rm;
    delete bpm;
    delete fm;
    
    return 0;
}
```

## 编译和运行

```bash
# 编译
cd record
make

# 运行测试
./test_record

# 运行简单示例
./simple_example
```

## 注意事项

1. **必须初始化**: 在使用前必须调用 `MyBitMap::initConst()`
2. **内存管理**: 不要手动释放 `allocPage` 或 `getPage` 返回的指针
3. **脏页标记**: 修改页面内容后必须调用 `markDirty(index)`
4. **记录ID唯一性**: 每个记录ID必须唯一，重复插入会失败
5. **关闭操作**: 程序结束前应调用 `close()` 确保数据写回磁盘

## 功能特点

- ✅ 支持可变长度记录
- ✅ 支持整数数组和字节数组（字符串）两种数据类型
- ✅ 自动页面分配和管理
- ✅ 记录的唯一性检查
- ✅ 完整的增删改查操作
- ✅ 记录遍历功能
- ✅ 统计信息查询

## 待完善功能

- [ ] 页面碎片整理（compactPage）
- [ ] 固定长度记录支持
- [ ] 记录索引（加速查询）
- [ ] 事务支持
- [ ] 并发控制

