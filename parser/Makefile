# Makefile for SQL Parser

CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2 -g
INCLUDES = -I./
OBJDIR = obj

# 源文件
PARSER_SOURCES = SimpleParser.cpp
TEST_SOURCES = test_parser.cpp

# 对象文件
PARSER_OBJECTS = $(PARSER_SOURCES:.cpp=.o)
TEST_OBJECTS = $(TEST_SOURCES:.cpp=.o)

# 目标文件
LIB_TARGET = libparser.a
TEST_TARGET = test_parser

.PHONY: all clean test antlr

all: $(TEST_TARGET)

# 创建对象文件目录
$(OBJDIR):
	mkdir -p $(OBJDIR)

# 编译 SimpleParser
$(OBJDIR)/SimpleParser.o: SimpleParser.cpp SimpleParser.h SQLStatement.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 编译测试程序
$(OBJDIR)/test_parser.o: test_parser.cpp SimpleParser.h SQLStatement.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 链接测试程序
$(TEST_TARGET): $(OBJDIR)/test_parser.o $(OBJDIR)/SimpleParser.o
	$(CXX) $(CXXFLAGS) -o $@ $^

# 创建静态库
$(LIB_TARGET): $(OBJDIR)/SimpleParser.o
	ar rcs $@ $^

lib: $(LIB_TARGET)

test: $(TEST_TARGET)
	./$(TEST_TARGET)

# ANTLR4 生成（需要先安装 ANTLR4 和 Java）
antlr:
	@echo "生成 ANTLR4 解析器..."
	@if [ ! -f antlr-4.13.1-complete.jar ]; then \
		echo "正在下载 ANTLR4..."; \
		wget -q https://www.antlr.org/download/antlr-4.13.1-complete.jar; \
	fi
	@mkdir -p generated
	java -jar antlr-4.13.1-complete.jar -Dlanguage=Cpp -visitor -no-listener ../SQL.g4 -o generated
	@echo "ANTLR4 解析器已生成到 generated/ 目录"

clean:
	rm -rf $(OBJDIR)
	rm -f $(TEST_TARGET) $(LIB_TARGET)





