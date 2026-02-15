# Makefile for Record Management System

CXX = g++
CXXFLAGS = -std=c++11 -Wall -O2
INCLUDES = -I./
SRCDIR = ./
OBJDIR = obj

# 源文件
RECORD_SOURCES = RecordManager.cpp
TEST_SOURCES = test_record.cpp simple_example.cpp

# 对象文件
RECORD_OBJECTS = $(RECORD_SOURCES:.cpp=.o)
TEST_OBJECTS = $(TEST_SOURCES:.cpp=.o)

# 目标文件
RECORD_TARGET = librecord.a
TEST_TARGET = test_record
SIMPLE_TARGET = simple_example

# 所有对象文件路径
ALL_OBJECTS = $(addprefix $(OBJDIR)/, $(RECORD_OBJECTS) $(TEST_OBJECTS))

.PHONY: all clean test

all: $(TEST_TARGET) $(SIMPLE_TARGET)

# 创建对象文件目录
$(OBJDIR):
	mkdir -p $(OBJDIR)

# 编译RecordManager
$(OBJDIR)/RecordManager.o: RecordManager.cpp RecordManager.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 编译测试程序
$(OBJDIR)/test_record.o: test_record.cpp RecordManager.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/simple_example.o: simple_example.cpp RecordManager.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 链接测试程序
$(TEST_TARGET): $(OBJDIR)/test_record.o $(OBJDIR)/RecordManager.o
	$(CXX) $(CXXFLAGS) -o $@ $^ -L./filesystem -lstdc++

$(SIMPLE_TARGET): $(OBJDIR)/simple_example.o $(OBJDIR)/RecordManager.o
	$(CXX) $(CXXFLAGS) -o $@ $^ -L./filesystem -lstdc++

test: $(TEST_TARGET)
	./$(TEST_TARGET)

clean:
	rm -rf $(OBJDIR)
	rm -f $(TEST_TARGET) $(SIMPLE_TARGET) $(RECORD_TARGET)
	rm -f record_db.dat simple_db.dat

