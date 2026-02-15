#include "../main/CommandExecutor.h"
#include <iostream>
#include <cassert>
#include <cstdlib>

// 测试辅助宏
#define TEST_CASE(name) std::cout << "\n=== Test: " << name << " ===" << std::endl
#define ASSERT_TRUE(cond, msg) do { \
    if (!(cond)) { \
        std::cerr << "FAILED: " << msg << std::endl; \
        return false; \
    } \
    std::cout << "  PASS: " << msg << std::endl; \
} while(0)

#define ASSERT_CONTAINS(result, substr, msg) do { \
    if (result.find(substr) == std::string::npos) { \
        std::cerr << "FAILED: " << msg << "\n  Result: " << result << std::endl; \
        return false; \
    } \
    std::cout << "  PASS: " << msg << std::endl; \
} while(0)

#define ASSERT_NOT_CONTAINS(result, substr, msg) do { \
    if (result.find(substr) != std::string::npos) { \
        std::cerr << "FAILED: " << msg << "\n  Result: " << result << std::endl; \
        return false; \
    } \
    std::cout << "  PASS: " << msg << std::endl; \
} while(0)

class DatabaseTest {
private:
    CommandExecutor* executor;
    std::string testDir;
    
public:
    DatabaseTest(const std::string& dir = "./test_data") : testDir(dir) {
        // 清理测试目录
        system(("rm -rf " + dir).c_str());
        executor = new CommandExecutor(dir);
    }
    
    ~DatabaseTest() {
        delete executor;
        // 清理测试目录
        system(("rm -rf " + testDir).c_str());
    }
    
    std::string exec(const std::string& sql) {
        std::cout << "  SQL: " << sql << std::endl;
        std::string result = executor->execute(sql);
        return result;
    }
    
    // 测试数据库操作
    bool testDatabaseOperations() {
        TEST_CASE("Database Operations");
        
        // 创建数据库
        std::string result = exec("CREATE DATABASE testdb");
        ASSERT_CONTAINS(result, "created", "Create database");
        
        // 显示数据库
        result = exec("SHOW DATABASES");
        ASSERT_CONTAINS(result, "testdb", "Show databases");
        
        // 使用数据库
        result = exec("USE testdb");
        ASSERT_CONTAINS(result, "changed", "Use database");
        
        // 创建另一个数据库
        result = exec("CREATE DATABASE testdb2");
        ASSERT_CONTAINS(result, "created", "Create another database");
        
        // 删除数据库
        result = exec("DROP DATABASE testdb2");
        ASSERT_CONTAINS(result, "dropped", "Drop database");
        
        // 确认删除
        result = exec("SHOW DATABASES");
        ASSERT_NOT_CONTAINS(result, "testdb2", "Database removed");
        
        return true;
    }
    
    // 测试表操作
    bool testTableOperations() {
        TEST_CASE("Table Operations");
        
        // 确保在数据库中
        exec("USE testdb");
        
        // 创建表
        std::string result = exec(
            "CREATE TABLE students ("
            "  id INT NOT NULL,"
            "  name VARCHAR(50),"
            "  age INT,"
            "  score FLOAT,"
            "  PRIMARY KEY (id)"
            ")"
        );
        ASSERT_CONTAINS(result, "created", "Create table");
        
        // 显示表
        result = exec("SHOW TABLES");
        ASSERT_CONTAINS(result, "students", "Show tables");
        
        // 描述表结构
        result = exec("DESC students");
        ASSERT_CONTAINS(result, "id", "Describe table - id column");
        ASSERT_CONTAINS(result, "name", "Describe table - name column");
        ASSERT_CONTAINS(result, "INT", "Describe table - INT type");
        ASSERT_CONTAINS(result, "VARCHAR", "Describe table - VARCHAR type");
        
        // 创建另一个表
        result = exec(
            "CREATE TABLE courses ("
            "  course_id INT NOT NULL,"
            "  course_name VARCHAR(100),"
            "  credits INT,"
            "  PRIMARY KEY (course_id)"
            ")"
        );
        ASSERT_CONTAINS(result, "created", "Create courses table");
        
        // 创建关联表
        result = exec(
            "CREATE TABLE enrollments ("
            "  student_id INT,"
            "  course_id INT,"
            "  grade FLOAT,"
            "  FOREIGN KEY (student_id) REFERENCES students(id),"
            "  FOREIGN KEY (course_id) REFERENCES courses(course_id)"
            ")"
        );
        ASSERT_CONTAINS(result, "created", "Create enrollments table with FK");
        
        return true;
    }
    
    // 测试INSERT操作
    bool testInsertOperations() {
        TEST_CASE("INSERT Operations");
        
        exec("USE testdb");
        
        // 插入单条记录
        std::string result = exec("INSERT INTO students VALUES (1, 'Alice', 20, 95.5)");
        ASSERT_CONTAINS(result, "OK", "Insert single record");
        
        // 插入多条记录
        result = exec("INSERT INTO students VALUES (2, 'Bob', 21, 88.0)");
        ASSERT_CONTAINS(result, "OK", "Insert second record");
        
        result = exec("INSERT INTO students VALUES (3, 'Charlie', 19, 92.3)");
        ASSERT_CONTAINS(result, "OK", "Insert third record");
        
        result = exec("INSERT INTO students VALUES (4, 'Diana', 22, 78.5)");
        ASSERT_CONTAINS(result, "OK", "Insert fourth record");
        
        result = exec("INSERT INTO students VALUES (5, 'Eve', 20, 85.0)");
        ASSERT_CONTAINS(result, "OK", "Insert fifth record");
        
        // 插入课程数据
        result = exec("INSERT INTO courses VALUES (101, 'Mathematics', 4)");
        ASSERT_CONTAINS(result, "OK", "Insert course");
        
        result = exec("INSERT INTO courses VALUES (102, 'Physics', 3)");
        ASSERT_CONTAINS(result, "OK", "Insert course 2");
        
        result = exec("INSERT INTO courses VALUES (103, 'Chemistry', 3)");
        ASSERT_CONTAINS(result, "OK", "Insert course 3");
        
        // 插入选课记录
        result = exec("INSERT INTO enrollments VALUES (1, 101, 95.0)");
        ASSERT_CONTAINS(result, "OK", "Insert enrollment");
        
        result = exec("INSERT INTO enrollments VALUES (1, 102, 88.0)");
        ASSERT_CONTAINS(result, "OK", "Insert enrollment 2");
        
        result = exec("INSERT INTO enrollments VALUES (2, 101, 82.0)");
        ASSERT_CONTAINS(result, "OK", "Insert enrollment 3");
        
        return true;
    }
    
    // 测试SELECT操作
    bool testSelectOperations() {
        TEST_CASE("SELECT Operations");
        
        exec("USE testdb");
        
        // 查询所有记录
        std::string result = exec("SELECT * FROM students");
        ASSERT_CONTAINS(result, "Alice", "Select all - Alice");
        ASSERT_CONTAINS(result, "Bob", "Select all - Bob");
        ASSERT_CONTAINS(result, "5 row", "Select all - 5 rows");
        
        // 查询特定列
        result = exec("SELECT name, score FROM students");
        ASSERT_CONTAINS(result, "name", "Select columns - name header");
        ASSERT_CONTAINS(result, "score", "Select columns - score header");
        
        // WHERE条件查询
        result = exec("SELECT * FROM students WHERE age > 20");
        ASSERT_CONTAINS(result, "Bob", "Where > - Bob (21)");
        ASSERT_CONTAINS(result, "Diana", "Where > - Diana (22)");
        ASSERT_NOT_CONTAINS(result, "Charlie", "Where > - Not Charlie (19)");
        
        // 等值查询
        result = exec("SELECT * FROM students WHERE name = 'Alice'");
        ASSERT_CONTAINS(result, "Alice", "Where = - Alice");
        ASSERT_CONTAINS(result, "1 row", "Where = - 1 row");
        
        // ORDER BY
        result = exec("SELECT * FROM students ORDER BY score DESC");
        ASSERT_CONTAINS(result, "Alice", "Order by DESC");
        
        // LIMIT
        result = exec("SELECT * FROM students LIMIT 2");
        ASSERT_CONTAINS(result, "2 row", "Limit 2");
        
        return true;
    }
    
    // 测试聚合函数
    bool testAggregateFunctions() {
        TEST_CASE("Aggregate Functions");
        
        exec("USE testdb");
        
        // COUNT
        std::string result = exec("SELECT COUNT(*) FROM students");
        ASSERT_CONTAINS(result, "5", "COUNT(*) = 5");
        
        // AVG
        result = exec("SELECT AVG(score) FROM students");
        ASSERT_CONTAINS(result, "87", "AVG(score)");
        
        // MAX
        result = exec("SELECT MAX(score) FROM students");
        ASSERT_CONTAINS(result, "95", "MAX(score)");
        
        // MIN
        result = exec("SELECT MIN(age) FROM students");
        ASSERT_CONTAINS(result, "19", "MIN(age)");
        
        // SUM
        result = exec("SELECT SUM(score) FROM students");
        ASSERT_CONTAINS(result, "439", "SUM(score)");
        
        return true;
    }
    
    // 测试UPDATE操作
    bool testUpdateOperations() {
        TEST_CASE("UPDATE Operations");
        
        exec("USE testdb");
        
        // 更新单条记录
        std::string result = exec("UPDATE students SET score = 96.0 WHERE name = 'Alice'");
        ASSERT_CONTAINS(result, "OK", "Update single record");
        
        // 验证更新
        result = exec("SELECT score FROM students WHERE name = 'Alice'");
        ASSERT_CONTAINS(result, "96", "Verify update");
        
        // 更新多条记录
        result = exec("UPDATE students SET age = 21 WHERE age = 20");
        ASSERT_CONTAINS(result, "OK", "Update multiple records");
        
        return true;
    }
    
    // 测试DELETE操作
    bool testDeleteOperations() {
        TEST_CASE("DELETE Operations");
        
        exec("USE testdb");
        
        // 先插入一条用于删除测试
        exec("INSERT INTO students VALUES (100, 'ToDelete', 25, 60.0)");
        
        // 删除记录
        std::string result = exec("DELETE FROM students WHERE id = 100");
        ASSERT_CONTAINS(result, "OK", "Delete record");
        
        // 验证删除
        result = exec("SELECT * FROM students WHERE id = 100");
        ASSERT_CONTAINS(result, "0 row", "Verify delete");
        
        return true;
    }
    
    // 测试JOIN操作
    bool testJoinOperations() {
        TEST_CASE("JOIN Operations");
        
        exec("USE testdb");
        
        // 简单JOIN
        std::string result = exec(
            "SELECT students.name, courses.course_name, enrollments.grade "
            "FROM students, courses, enrollments "
            "WHERE students.id = enrollments.student_id "
            "AND courses.course_id = enrollments.course_id"
        );
        ASSERT_CONTAINS(result, "Alice", "Join - student name");
        ASSERT_CONTAINS(result, "Mathematics", "Join - course name");
        
        return true;
    }
    
    // 测试索引操作
    bool testIndexOperations() {
        TEST_CASE("Index Operations");
        
        exec("USE testdb");
        
        // 创建索引
        std::string result = exec("ALTER TABLE students ADD INDEX (name)");
        ASSERT_CONTAINS(result, "created", "Create index");
        
        // 显示索引
        result = exec("SHOW INDEXES");
        ASSERT_CONTAINS(result, "students", "Show indexes - table");
        ASSERT_CONTAINS(result, "name", "Show indexes - column");
        
        // 删除索引
        result = exec("ALTER TABLE students DROP INDEX name");
        ASSERT_CONTAINS(result, "dropped", "Drop index");
        
        return true;
    }
    
    // 测试删除表
    bool testDropTable() {
        TEST_CASE("Drop Table");
        
        exec("USE testdb");
        
        // 删除表
        std::string result = exec("DROP TABLE enrollments");
        ASSERT_CONTAINS(result, "dropped", "Drop enrollments table");
        
        result = exec("DROP TABLE courses");
        ASSERT_CONTAINS(result, "dropped", "Drop courses table");
        
        result = exec("DROP TABLE students");
        ASSERT_CONTAINS(result, "dropped", "Drop students table");
        
        // 验证删除
        result = exec("SHOW TABLES");
        ASSERT_NOT_CONTAINS(result, "students", "Verify tables dropped");
        
        return true;
    }
    
    // 运行所有测试
    bool runAllTests() {
        std::cout << "\n======================================" << std::endl;
        std::cout << "    SimpleDB Test Suite" << std::endl;
        std::cout << "======================================" << std::endl;
        
        int passed = 0;
        int failed = 0;
        
        if (testDatabaseOperations()) passed++; else failed++;
        if (testTableOperations()) passed++; else failed++;
        if (testInsertOperations()) passed++; else failed++;
        if (testSelectOperations()) passed++; else failed++;
        if (testAggregateFunctions()) passed++; else failed++;
        if (testUpdateOperations()) passed++; else failed++;
        if (testDeleteOperations()) passed++; else failed++;
        if (testJoinOperations()) passed++; else failed++;
        if (testIndexOperations()) passed++; else failed++;
        if (testDropTable()) passed++; else failed++;
        
        std::cout << "\n======================================" << std::endl;
        std::cout << "Test Results: " << passed << " passed, " << failed << " failed" << std::endl;
        std::cout << "======================================" << std::endl;
        
        return failed == 0;
    }
};

int main() {
    DatabaseTest test;
    bool success = test.runAllTests();
    return success ? 0 : 1;
}

