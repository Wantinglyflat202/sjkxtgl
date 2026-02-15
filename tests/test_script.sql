-- SimpleDB 测试脚本
-- 测试所有基本功能：数据库操作、表操作、CRUD操作、索引和约束

-- ==================== 数据库操作 ====================
SHOW DATABASES;
CREATE DATABASE test_db;
SHOW DATABASES;
USE test_db;

-- ==================== 表操作 ====================
-- 创建学生表
CREATE TABLE students (
    id INT NOT NULL,
    name VARCHAR(50) NOT NULL,
    age INT,
    PRIMARY KEY (id)
);

-- 创建课程表
CREATE TABLE courses (
    course_id INT NOT NULL,
    course_name VARCHAR(100) NOT NULL,
    credits INT,
    PRIMARY KEY (course_id)
);

-- 创建选课表（包含外键）
CREATE TABLE enrollments (
    enrollment_id INT NOT NULL,
    student_id INT NOT NULL,
    course_id INT NOT NULL,
    grade FLOAT,
    PRIMARY KEY (enrollment_id)
);

SHOW TABLES;
DESC students;
DESC courses;
DESC enrollments;

-- ==================== 插入数据 ====================
INSERT INTO students VALUES (1, 'Alice', 20);
INSERT INTO students VALUES (2, 'Bob', 22);
INSERT INTO students VALUES (3, 'Charlie', 21);
INSERT INTO students VALUES (4, 'Diana', 19);
INSERT INTO students VALUES (5, 'Edward', 23);

INSERT INTO courses VALUES (101, 'Database Systems', 3);
INSERT INTO courses VALUES (102, 'Operating Systems', 4);
INSERT INTO courses VALUES (103, 'Computer Networks', 3);

INSERT INTO enrollments VALUES (1, 1, 101, 85.5);
INSERT INTO enrollments VALUES (2, 1, 102, 90.0);
INSERT INTO enrollments VALUES (3, 2, 101, 78.5);
INSERT INTO enrollments VALUES (4, 3, 102, 92.0);
INSERT INTO enrollments VALUES (5, 3, 103, 88.0);

-- ==================== 查询数据 ====================
-- 基本查询
SELECT * FROM students;
SELECT * FROM courses;
SELECT * FROM enrollments;

-- 条件查询
SELECT * FROM students WHERE age > 20;
SELECT * FROM students WHERE id = 3;
SELECT * FROM enrollments WHERE grade >= 90;

-- 多条件查询
SELECT * FROM students WHERE age >= 20 AND age <= 22;

-- 聚合函数
SELECT COUNT(*) FROM students;
SELECT AVG(grade) FROM enrollments;
SELECT MAX(age) FROM students;
SELECT MIN(age) FROM students;
SELECT SUM(credits) FROM courses;

-- ==================== 更新数据 ====================
UPDATE students SET age = 24 WHERE id = 5;
SELECT * FROM students WHERE id = 5;

UPDATE enrollments SET grade = 95.0 WHERE enrollment_id = 1;
SELECT * FROM enrollments WHERE enrollment_id = 1;

-- ==================== 删除数据 ====================
DELETE FROM students WHERE id = 4;
SELECT * FROM students;

-- ==================== 索引操作 ====================
ALTER TABLE students ADD INDEX (age);
SHOW INDEXES;
ALTER TABLE students DROP INDEX age;

-- ==================== 清理 ====================
DROP TABLE enrollments;
DROP TABLE courses;
DROP TABLE students;
SHOW TABLES;

DROP DATABASE test_db;
SHOW DATABASES;

-- 测试完成
