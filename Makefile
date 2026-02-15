# SimpleDB Makefile with ANTLR4 Support

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -g
LDFLAGS = 
SRC_DIR = .
OBJ_DIR = obj
BIN_DIR = bin

# ANTLR4 Runtime 目录
ANTLR4_RUNTIME_DIR = antlr4-runtime/runtime/src
ANTLR4_INCLUDE = -I$(ANTLR4_RUNTIME_DIR) -Iparser/generated -Iparser
CXXFLAGS += $(ANTLR4_INCLUDE)
ANTLR4_SRCS = $(shell find $(ANTLR4_RUNTIME_DIR) -name "*.cpp")
ANTLR4_OBJS = $(patsubst $(ANTLR4_RUNTIME_DIR)/%.cpp,$(OBJ_DIR)/antlr4/%.o,$(ANTLR4_SRCS))
GENERATED_SRCS = parser/generated/SQLLexer.cpp \
                 parser/generated/SQLParser.cpp \
                 parser/generated/SQLBaseVisitor.cpp \
                 parser/generated/SQLVisitor.cpp
GENERATED_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(GENERATED_SRCS))
PARSER_SRCS = parser/ANTLRParser.cpp parser/SQLStatementVisitor.cpp
RECORD_SRCS = record/RecordManager.cpp
INDEX_SRCS = index/BPlusTree.cpp index/IndexManager.cpp
SYSTEM_SRCS = system/SystemManager.cpp
QUERY_SRCS = query/QueryExecutor.cpp
MAIN_SRCS = main/CommandExecutor.cpp main/main.cpp
ALL_SRCS = $(PARSER_SRCS) $(RECORD_SRCS) $(INDEX_SRCS) $(SYSTEM_SRCS) $(QUERY_SRCS) $(MAIN_SRCS)
PARSER_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(PARSER_SRCS))
RECORD_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(RECORD_SRCS))
INDEX_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(INDEX_SRCS))
SYSTEM_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(SYSTEM_SRCS))
QUERY_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(QUERY_SRCS))
MAIN_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(MAIN_SRCS))

ALL_OBJS = $(ANTLR4_OBJS) $(GENERATED_OBJS) $(PARSER_OBJS) $(RECORD_OBJS) $(INDEX_OBJS) $(SYSTEM_OBJS) $(QUERY_OBJS) $(MAIN_OBJS)
TARGET = $(BIN_DIR)/simpledb
.PHONY: all clean test dirs antlr4-gen
all: dirs $(TARGET)
dirs:
	@mkdir -p $(OBJ_DIR)/parser
	@mkdir -p $(OBJ_DIR)/parser/generated
	@mkdir -p $(OBJ_DIR)/record
	@mkdir -p $(OBJ_DIR)/index
	@mkdir -p $(OBJ_DIR)/system
	@mkdir -p $(OBJ_DIR)/query
	@mkdir -p $(OBJ_DIR)/main
	@mkdir -p $(OBJ_DIR)/antlr4
	@mkdir -p $(OBJ_DIR)/antlr4/atn
	@mkdir -p $(OBJ_DIR)/antlr4/dfa
	@mkdir -p $(OBJ_DIR)/antlr4/misc
	@mkdir -p $(OBJ_DIR)/antlr4/support
	@mkdir -p $(OBJ_DIR)/antlr4/tree
	@mkdir -p $(OBJ_DIR)/antlr4/tree/pattern
	@mkdir -p $(OBJ_DIR)/antlr4/tree/xpath
	@mkdir -p $(OBJ_DIR)/antlr4/internal
	@mkdir -p $(BIN_DIR)
	@mkdir -p data
antlr4-gen:
	java -jar tools/antlr-4.13.2-complete.jar -Dlanguage=Cpp -visitor -no-listener -o parser/generated SQL.g4
$(TARGET): $(ALL_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Build completed: $@"

# 编译ANTLR4 Runtime
$(OBJ_DIR)/antlr4/%.o: $(ANTLR4_RUNTIME_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<
$(OBJ_DIR)/parser/generated/%.o: parser/generated/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<
$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<
test: dirs $(TEST_TARGET)
	./$(TEST_TARGET)
TEST_SRCS = tests/test_db.cpp
TEST_OBJS = $(patsubst %.cpp,$(OBJ_DIR)/%.o,$(TEST_SRCS))
CORE_OBJS = $(ANTLR4_OBJS) $(GENERATED_OBJS) $(PARSER_OBJS) $(RECORD_OBJS) $(INDEX_OBJS) $(SYSTEM_OBJS) $(QUERY_OBJS) $(OBJ_DIR)/main/CommandExecutor.o
$(TEST_TARGET): $(CORE_OBJS) $(OBJ_DIR)/tests/test_db.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
$(OBJ_DIR)/tests/test_db.o: tests/test_db.cpp
	@mkdir -p $(OBJ_DIR)/tests
	$(CXX) $(CXXFLAGS) -c -o $@ $<
clean:
	rm -rf $(OBJ_DIR)
	rm -rf $(BIN_DIR)
	rm -rf data
run: $(TARGET)
	./$(TARGET)
run-file: $(TARGET)
	./$(TARGET) -f $(FILE)
$(OBJ_DIR)/parser/ANTLRParser.o: parser/ANTLRParser.cpp parser/ANTLRParser.h parser/SQLStatement.h parser/SQLStatementVisitor.h
$(OBJ_DIR)/parser/SQLStatementVisitor.o: parser/SQLStatementVisitor.cpp parser/SQLStatementVisitor.h parser/SQLStatement.h
$(OBJ_DIR)/record/RecordManager.o: record/RecordManager.cpp record/RecordManager.h
$(OBJ_DIR)/index/BPlusTree.o: index/BPlusTree.cpp index/BPlusTree.h
$(OBJ_DIR)/index/IndexManager.o: index/IndexManager.cpp index/IndexManager.h index/BPlusTree.h
$(OBJ_DIR)/system/SystemManager.o: system/SystemManager.cpp system/SystemManager.h
$(OBJ_DIR)/query/QueryExecutor.o: query/QueryExecutor.cpp query/QueryExecutor.h
$(OBJ_DIR)/main/CommandExecutor.o: main/CommandExecutor.cpp main/CommandExecutor.h
$(OBJ_DIR)/main/main.o: main/main.cpp main/CommandExecutor.h
