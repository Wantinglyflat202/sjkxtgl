#!/bin/bash

# SimpleDB Test Runner Script
# 用于编译和运行所有测试

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}    SimpleDB Test Runner${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# 进入项目目录
cd "$(dirname "$0")/.."

# 清理之前的构建
echo -e "${YELLOW}Cleaning previous build...${NC}"
make clean 2>/dev/null || true
rm -rf test_data 2>/dev/null || true

# 编译项目
echo -e "${YELLOW}Building project...${NC}"
make all

# 检查编译结果
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Build successful!${NC}"
else
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi

# 编译测试
echo -e "${YELLOW}Building tests...${NC}"
make test

# 运行测试
echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}    Running Unit Tests${NC}"
echo -e "${YELLOW}========================================${NC}"
./bin/test_db

if [ $? -eq 0 ]; then
    echo -e "${GREEN}All unit tests passed!${NC}"
else
    echo -e "${RED}Some unit tests failed!${NC}"
    exit 1
fi

# 运行SQL脚本测试（可选）
echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}    Running SQL Script Test${NC}"
echo -e "${YELLOW}========================================${NC}"
./bin/simpledb -f tests/test_script.sql

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}    All Tests Completed!${NC}"
echo -e "${GREEN}========================================${NC}"

