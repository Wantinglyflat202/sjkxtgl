#ifndef SQL_STATEMENT_VISITOR_H
#define SQL_STATEMENT_VISITOR_H

#include "generated/SQLBaseVisitor.h"
#include "generated/SQLParser.h"
#include "SQLStatement.h"
#include <string>
#include <vector>
#include <any>

/*
 * SQL语句访问器
 * 遍历ANTLR4生成的解析树，将其转换为SQLStatement结构
 */
class SQLStatementVisitor : public SQLBaseVisitor {
private:
    std::string errorMsg;
    
    // 辅助函数：去除字符串引号
    std::string stripQuotes(const std::string& str) {
        if (str.length() >= 2 && (str[0] == '\'' || str[0] == '"')) {
            return str.substr(1, str.length() - 2);
        }
        return str;
    }
    
    // 解析Value节点
    Value parseValueNode(SQLParser::ValueContext* ctx);
    
    // 解析Column节点
    Column parseColumnNode(SQLParser::ColumnContext* ctx);
    
    // 解析比较操作符
    CompareOp parseOperatorNode(SQLParser::Operator_Context* ctx);
    
    // 解析聚合函数类型
    AggregateType parseAggregatorNode(SQLParser::AggregatorContext* ctx);

public:
    SQLStatementVisitor() = default;
    
    std::string getError() const { return errorMsg; }
    
    // ==================== 数据库语句 ====================
    
    std::any visitCreate_db(SQLParser::Create_dbContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::CREATE_DATABASE;
        stmt.databaseName = ctx->Identifier()->getText();
        stmt.valid = true;
        return stmt;
    }
    
    std::any visitDrop_db(SQLParser::Drop_dbContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::DROP_DATABASE;
        stmt.databaseName = ctx->Identifier()->getText();
        stmt.valid = true;
        return stmt;
    }
    
    std::any visitShow_dbs(SQLParser::Show_dbsContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::SHOW_DATABASES;
        stmt.valid = true;
        return stmt;
    }
    
    std::any visitUse_db(SQLParser::Use_dbContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::USE_DATABASE;
        stmt.databaseName = ctx->Identifier()->getText();
        stmt.valid = true;
        return stmt;
    }
    
    std::any visitShow_tables(SQLParser::Show_tablesContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::SHOW_TABLES;
        stmt.valid = true;
        return stmt;
    }
    
    std::any visitShow_indexes(SQLParser::Show_indexesContext* ctx) override {
        SQLStatement stmt;
        stmt.type = SQLType::SHOW_INDEXES;
        stmt.valid = true;
        return stmt;
    }
    
    // ==================== 表语句 ====================
    
    std::any visitCreate_table(SQLParser::Create_tableContext* ctx) override;
    std::any visitDrop_table(SQLParser::Drop_tableContext* ctx) override;
    std::any visitDescribe_table(SQLParser::Describe_tableContext* ctx) override;
    std::any visitInsert_into_table(SQLParser::Insert_into_tableContext* ctx) override;
    std::any visitDelete_from_table(SQLParser::Delete_from_tableContext* ctx) override;
    std::any visitUpdate_table(SQLParser::Update_tableContext* ctx) override;
    std::any visitSelect_table_(SQLParser::Select_table_Context* ctx) override;
    std::any visitSelect_table(SQLParser::Select_tableContext* ctx) override;
    std::any visitLoad_table(SQLParser::Load_tableContext* ctx) override;
    
    // ==================== ALTER语句 ====================
    
    std::any visitAlter_add_index(SQLParser::Alter_add_indexContext* ctx) override;
    std::any visitAlter_drop_index(SQLParser::Alter_drop_indexContext* ctx) override;
    std::any visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext* ctx) override;
    std::any visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext* ctx) override;
    std::any visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext* ctx) override;
    std::any visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext* ctx) override;
    std::any visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext* ctx) override;
    
    // ==================== 字段定义 ====================
    
    std::any visitNormal_field(SQLParser::Normal_fieldContext* ctx) override;
    std::any visitPrimary_key_field(SQLParser::Primary_key_fieldContext* ctx) override;
    std::any visitForeign_key_field(SQLParser::Foreign_key_fieldContext* ctx) override;
    
    // ==================== 其他节点 ====================
    
    std::any visitValue(SQLParser::ValueContext* ctx) override;
    std::any visitValue_list(SQLParser::Value_listContext* ctx) override;
    std::any visitColumn(SQLParser::ColumnContext* ctx) override;
    std::any visitWhere_and_clause(SQLParser::Where_and_clauseContext* ctx) override;
    std::any visitWhere_operator_expression(SQLParser::Where_operator_expressionContext* ctx) override;
    std::any visitWhere_null(SQLParser::Where_nullContext* ctx) override;
    std::any visitWhere_in_list(SQLParser::Where_in_listContext* ctx) override;
    std::any visitWhere_like_string(SQLParser::Where_like_stringContext* ctx) override;
    std::any visitSelector(SQLParser::SelectorContext* ctx) override;
    std::any visitSelectors(SQLParser::SelectorsContext* ctx) override;
    std::any visitIdentifiers(SQLParser::IdentifiersContext* ctx) override;
    std::any visitSet_clause(SQLParser::Set_clauseContext* ctx) override;
    std::any visitType_(SQLParser::Type_Context* ctx) override;
};

#endif // SQL_STATEMENT_VISITOR_H

