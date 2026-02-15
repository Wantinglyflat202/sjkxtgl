# SQL 解析器模块

## 概述

本模块使用 **ANTLR4** 自动生成的解析器来解析 SQL 语句。

## 文件结构

```
parser/
├── ANTLRParser.h/cpp       # ANTLR4解析器包装类
├── SQLStatementVisitor.h/cpp # AST访问器（将解析树转换为SQLStatement）
├── SQLStatement.h          # SQL语句结构定义
├── generated/              # ANTLR4生成的代码
│   ├── SQLLexer.h/cpp     # 词法分析器
│   ├── SQLParser.h/cpp    # 语法分析器
│   ├── SQLVisitor.h/cpp   # Visitor接口
│   └── SQLBaseVisitor.h/cpp # Visitor基类
└── README.md
```

## 使用方法

```cpp
#include "ANTLRParser.h"

// 使用ANTLRParser（也可以使用SimpleParser别名）
ANTLRParser parser;
SQLStatement stmt = parser.parse("SELECT * FROM users WHERE id = 1");

if (stmt.isValid()) {
    // 处理解析结果
} else {
    std::cerr << stmt.getError() << std::endl;
}
```

## 重新生成解析器

如果修改了 `SQL.g4` 语法文件，需要重新生成解析器代码：

```bash
# 在项目根目录下
make antlr4-gen

# 或者手动运行
java -jar tools/antlr-4.13.2-complete.jar -Dlanguage=Cpp -visitor -no-listener -o parser/generated SQL.g4
```

## 支持的 SQL 语句

### 数据库操作
- `CREATE DATABASE dbname`
- `DROP DATABASE dbname`
- `SHOW DATABASES`
- `USE dbname`
- `SHOW TABLES`
- `SHOW INDEXES`

### 表操作
- `CREATE TABLE name (columns...)`
- `DROP TABLE name`
- `DESC tablename`

### 数据操作
- `INSERT INTO table VALUES (...)`
- `DELETE FROM table [WHERE ...]`
- `UPDATE table SET ... WHERE ...`
- `SELECT ... FROM ... [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT ...]`

### ALTER操作
- `ALTER TABLE ... ADD INDEX (columns)`
- `ALTER TABLE ... DROP INDEX name`
- `ALTER TABLE ... ADD PRIMARY KEY (columns)`
- `ALTER TABLE ... DROP PRIMARY KEY`
- `ALTER TABLE ... ADD FOREIGN KEY ... REFERENCES ...`
- `ALTER TABLE ... DROP FOREIGN KEY name`
- `ALTER TABLE ... ADD UNIQUE (columns)`

### 其他
- `LOAD DATA INFILE 'file' INTO TABLE t FIELDS TERMINATED BY ','`
