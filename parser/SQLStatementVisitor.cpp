#include "SQLStatementVisitor.h"
#include <cstdlib>
Value SQLStatementVisitor::parseValueNode(SQLParser::ValueContext* ctx) {
    if (!ctx) return Value::makeNull();
    
    if (ctx->Null()) {
        return Value::makeNull();
    } else if (ctx->Integer()) {
        return Value(std::stoi(ctx->Integer()->getText()));
    } else if (ctx->Float()) {
        return Value(std::stod(ctx->Float()->getText()));
    } else if (ctx->String()) {
        return Value(stripQuotes(ctx->String()->getText()));
    }
    
    return Value::makeNull();
}

Column SQLStatementVisitor::parseColumnNode(SQLParser::ColumnContext* ctx) {
    Column col;
    if (!ctx) return col;
    
    auto identifiers = ctx->Identifier();
    if (identifiers.size() == 2) {
        col.tableName = identifiers[0]->getText();
        col.columnName = identifiers[1]->getText();
    } else if (identifiers.size() == 1) {
        col.columnName = identifiers[0]->getText();
    }
    
    return col;
}

CompareOp SQLStatementVisitor::parseOperatorNode(SQLParser::Operator_Context* ctx) {
    if (!ctx) return CompareOp::EQ;
    
    if (ctx->EqualOrAssign()) return CompareOp::EQ;
    if (ctx->Less()) return CompareOp::LT;
    if (ctx->LessEqual()) return CompareOp::LE;
    if (ctx->Greater()) return CompareOp::GT;
    if (ctx->GreaterEqual()) return CompareOp::GE;
    if (ctx->NotEqual()) return CompareOp::NE;
    
    return CompareOp::EQ;
}

AggregateType SQLStatementVisitor::parseAggregatorNode(SQLParser::AggregatorContext* ctx) {
    if (!ctx) return AggregateType::NONE;
    
    if (ctx->Count()) return AggregateType::COUNT;
    if (ctx->Average()) return AggregateType::AVG;
    if (ctx->Max()) return AggregateType::MAX;
    if (ctx->Min()) return AggregateType::MIN;
    if (ctx->Sum()) return AggregateType::SUM;
    
    return AggregateType::NONE;
}
std::any SQLStatementVisitor::visitCreate_table(SQLParser::Create_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::CREATE_TABLE;
    stmt.tableName = ctx->Identifier()->getText();

    auto fieldList = ctx->field_list();
    if (fieldList) {
        for (auto field : fieldList->field()) {
            auto result = field->accept(this);
            if (auto normalField = dynamic_cast<SQLParser::Normal_fieldContext*>(field)) {
                ColumnDef col = std::any_cast<ColumnDef>(result);
                stmt.columns.push_back(col);
            } else if (auto pkField = dynamic_cast<SQLParser::Primary_key_fieldContext*>(field)) {
                KeyDef pk = std::any_cast<KeyDef>(result);
                stmt.primaryKey = pk;
            } else if (auto fkField = dynamic_cast<SQLParser::Foreign_key_fieldContext*>(field)) {
                KeyDef fk = std::any_cast<KeyDef>(result);
                stmt.foreignKeys.push_back(fk);
            }
        }
    }
    
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitDrop_table(SQLParser::Drop_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::DROP_TABLE;
    stmt.tableName = ctx->Identifier()->getText();
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitDescribe_table(SQLParser::Describe_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::DESC_TABLE;
    stmt.tableName = ctx->Identifier()->getText();
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::INSERT;
    stmt.tableName = ctx->Identifier()->getText();
    
    auto valueLists = ctx->value_lists();
    if (valueLists) {
        for (auto valueList : valueLists->value_list()) {
            auto result = valueList->accept(this);
            std::vector<Value> values = std::any_cast<std::vector<Value>>(result);
            stmt.valueLists.push_back(values);
        }
    }
    
    stmt.valid = !stmt.valueLists.empty();
    return stmt;
}

std::any SQLStatementVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::DELETE;
    stmt.tableName = ctx->Identifier()->getText();
    
    if (ctx->where_and_clause()) {
        auto result = ctx->where_and_clause()->accept(this);
        stmt.whereClauses = std::any_cast<std::vector<WhereClause>>(result);
    }
    
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitUpdate_table(SQLParser::Update_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::UPDATE;
    stmt.tableName = ctx->Identifier()->getText();
    
    // 解析SET子句
    if (ctx->set_clause()) {
        auto result = ctx->set_clause()->accept(this);
        stmt.setClauses = std::any_cast<std::vector<SetClause>>(result);
    }
    
    // 解析WHERE子句
    if (ctx->where_and_clause()) {
        auto result = ctx->where_and_clause()->accept(this);
        stmt.whereClauses = std::any_cast<std::vector<WhereClause>>(result);
    }
    
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitSelect_table_(SQLParser::Select_table_Context* ctx) {
    return ctx->select_table()->accept(this);
}

std::any SQLStatementVisitor::visitSelect_table(SQLParser::Select_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::SELECT;
    
    // 解析selectors
    if (ctx->selectors()) {
        auto result = ctx->selectors()->accept(this);
        stmt.selectors = std::any_cast<std::vector<Selector>>(result);
    }
    
    // 解析FROM表名列表
    if (ctx->identifiers()) {
        auto result = ctx->identifiers()->accept(this);
        stmt.fromTables = std::any_cast<std::vector<std::string>>(result);
    }
    
    // 解析WHERE子句
    if (ctx->where_and_clause()) {
        auto result = ctx->where_and_clause()->accept(this);
        stmt.whereClauses = std::any_cast<std::vector<WhereClause>>(result);
    }
    
    // 解析GROUP BY
    auto columns = ctx->column();
    size_t colIdx = 0;
    std::string text = ctx->getText();
    if (text.find("GROUPBY") != std::string::npos || text.find("GROUP") != std::string::npos) {
        // 寻找GROUP BY关键字后的列
        for (size_t i = 0; i < ctx->children.size(); i++) {
            if (ctx->children[i]->getText() == "GROUP") {
                // 下一个应该是BY，再下一个是column
                stmt.hasGroupBy = true;
                if (colIdx < columns.size()) {
                    stmt.groupByColumn = parseColumnNode(columns[colIdx]);
                    colIdx++;
                }
                break;
            }
        }
    }
    
    // 检查是否有ORDER BY
    if (text.find("ORDERBY") != std::string::npos || text.find("ORDER") != std::string::npos) {
        for (size_t i = 0; i < ctx->children.size(); i++) {
            if (ctx->children[i]->getText() == "ORDER") {
                stmt.hasOrderBy = true;
                if (colIdx < columns.size()) {
                    stmt.orderByColumn = parseColumnNode(columns[colIdx]);
                    colIdx++;
                }
                break;
            }
        }
        
        // 检查排序方式
        if (ctx->order()) {
            std::string orderText = ctx->order()->getText();
            if (orderText == "DESC") {
                stmt.orderType = OrderType::DESC;
            } else {
                stmt.orderType = OrderType::ASC;
            }
        }
    }
    auto integers = ctx->Integer();
    if (!integers.empty()) {
        stmt.hasLimit = true;
        stmt.limit = std::stoi(integers[0]->getText());
        if (integers.size() > 1) {
            stmt.offset = std::stoi(integers[1]->getText());
        }
    }
    
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitLoad_table(SQLParser::Load_tableContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::LOAD_DATA;
    
    auto strings = ctx->String();
    if (strings.size() >= 1) {
        stmt.fileName = stripQuotes(strings[0]->getText());
    }
    if (strings.size() >= 2) {
        stmt.delimiter = stripQuotes(strings[1]->getText());
    }
    
    stmt.tableName = ctx->Identifier()->getText();
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_add_index(SQLParser::Alter_add_indexContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_ADD_INDEX;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.indexName = identifiers[1]->getText();
    }
    if (ctx->identifiers()) {
        auto result = ctx->identifiers()->accept(this);
        stmt.indexColumns = std::any_cast<std::vector<std::string>>(result);
    }
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_drop_index(SQLParser::Alter_drop_indexContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_DROP_INDEX;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.indexName = identifiers[1]->getText();
    }
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_ADD_PRIMARY_KEY;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.constraintName = identifiers[1]->getText();
    }
    if (ctx->identifiers()) {
        auto result = ctx->identifiers()->accept(this);
        stmt.indexColumns = std::any_cast<std::vector<std::string>>(result);
    }
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_DROP_PRIMARY_KEY;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.constraintName = identifiers[1]->getText();
    }
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_ADD_FOREIGN_KEY;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    size_t refTableIdx = 1;
    if (identifiers.size() > 2) {
        stmt.constraintName = identifiers[1]->getText();
        refTableIdx = 2;
    }
    if (refTableIdx < identifiers.size()) {
        stmt.refTableName = identifiers[refTableIdx]->getText();
    }
    auto idLists = ctx->identifiers();
    if (idLists.size() >= 1) {
        auto result = idLists[0]->accept(this);
        stmt.indexColumns = std::any_cast<std::vector<std::string>>(result);
    }
    if (idLists.size() >= 2) {
        auto result = idLists[1]->accept(this);
        stmt.refColumns = std::any_cast<std::vector<std::string>>(result);
    }
    
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_DROP_FOREIGN_KEY;
    
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.constraintName = identifiers[1]->getText();
    }
    
    stmt.valid = true;
    return stmt;
}

std::any SQLStatementVisitor::visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext* ctx) {
    SQLStatement stmt;
    stmt.type = SQLType::ALTER_ADD_UNIQUE;
    auto identifiers = ctx->Identifier();
    stmt.tableName = identifiers[0]->getText();
    if (identifiers.size() > 1) {
        stmt.indexName = identifiers[1]->getText();
    }
    if (ctx->identifiers()) {
        auto result = ctx->identifiers()->accept(this);
        stmt.indexColumns = std::any_cast<std::vector<std::string>>(result);
    }
    stmt.valid = true;
    return stmt;
}
std::any SQLStatementVisitor::visitNormal_field(SQLParser::Normal_fieldContext* ctx) {
    ColumnDef col;
    col.name = ctx->Identifier()->getText();
    if (ctx->type_()) {
        std::string typeText = ctx->type_()->getText();
        if (typeText == "INT") {
            col.type = DataType::INT;
        } else if (typeText == "FLOAT") {
            col.type = DataType::FLOAT;
        } else if (typeText.find("VARCHAR") != std::string::npos) {
            col.type = DataType::VARCHAR;
            if (ctx->type_()->Integer()) {
                col.length = std::stoi(ctx->type_()->Integer()->getText());
            }
        }
    }
    if (ctx->Null()) {
        col.notNull = true;
    }
    if (ctx->value()) {
        col.hasDefault = true;
        col.defaultValue = parseValueNode(ctx->value());
    }
    return col;
}
std::any SQLStatementVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext* ctx) {
    KeyDef pk;
    if (ctx->Identifier()) {
        pk.name = ctx->Identifier()->getText();
    }
    if (ctx->identifiers()) {
        auto result = ctx->identifiers()->accept(this);
        pk.columns = std::any_cast<std::vector<std::string>>(result);
    }
    return pk;
}
std::any SQLStatementVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext* ctx) {
    KeyDef fk;
    auto identifiers = ctx->Identifier();
    if (identifiers.size() >= 1) {
        fk.name = identifiers[0]->getText();
    }
    if (identifiers.size() >= 2) {
        fk.refTable = identifiers[1]->getText();
    } else if (identifiers.size() == 1) {
        fk.refTable = identifiers[0]->getText();
        fk.name = "";
    }
    auto idLists = ctx->identifiers();
    if (idLists.size() >= 1) {
        auto result = idLists[0]->accept(this);
        fk.columns = std::any_cast<std::vector<std::string>>(result);
    }
    if (idLists.size() >= 2) {
        auto result = idLists[1]->accept(this);
        fk.refColumns = std::any_cast<std::vector<std::string>>(result);
    }
    return fk;
}
std::any SQLStatementVisitor::visitValue(SQLParser::ValueContext* ctx) {
    return parseValueNode(ctx);
}
std::any SQLStatementVisitor::visitValue_list(SQLParser::Value_listContext* ctx) {
    std::vector<Value> values;
    for (auto valCtx : ctx->value()) {
        values.push_back(parseValueNode(valCtx));
    }
    return values;
}
std::any SQLStatementVisitor::visitColumn(SQLParser::ColumnContext* ctx) {
    return parseColumnNode(ctx);
}
std::any SQLStatementVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext* ctx) {
    std::vector<WhereClause> clauses;
    for (auto whereCtx : ctx->where_clause()) {
        auto result = whereCtx->accept(this);
        clauses.push_back(std::any_cast<WhereClause>(result));
    }
    return clauses;
}
std::any SQLStatementVisitor::visitWhere_operator_expression(SQLParser::Where_operator_expressionContext* ctx) {
    WhereClause wc;
    wc.column = parseColumnNode(ctx->column());
    wc.op = parseOperatorNode(ctx->operator_());
    auto expr = ctx->expression();
    if (expr) {
        if (expr->value()) {
            wc.value = parseValueNode(expr->value());
            wc.isColumnCompare = false;
        } else if (expr->column()) {
            wc.rightColumn = parseColumnNode(expr->column());
            wc.isColumnCompare = true;
        }
    }
    return wc;
}
std::any SQLStatementVisitor::visitWhere_null(SQLParser::Where_nullContext* ctx) {
    WhereClause wc;
    wc.column = parseColumnNode(ctx->column());
    std::string text = ctx->getText();
    if (text.find("NOT") != std::string::npos) {
        wc.op = CompareOp::IS_NOT_NULL;
    } else {
        wc.op = CompareOp::IS_NULL;
    }
    return wc;
}
std::any SQLStatementVisitor::visitWhere_in_list(SQLParser::Where_in_listContext* ctx) {
    WhereClause wc;
    wc.column = parseColumnNode(ctx->column());
    wc.op = CompareOp::IN;
    if (ctx->value_list()) {
        auto result = ctx->value_list()->accept(this);
        wc.inList = std::any_cast<std::vector<Value>>(result);
    }
    return wc;
}
std::any SQLStatementVisitor::visitWhere_like_string(SQLParser::Where_like_stringContext* ctx) {
    WhereClause wc;
    wc.column = parseColumnNode(ctx->column());
    wc.op = CompareOp::LIKE;
    if (ctx->String()) {
        wc.value = Value(stripQuotes(ctx->String()->getText()));
    }
    return wc;
}
std::any SQLStatementVisitor::visitSelector(SQLParser::SelectorContext* ctx) {
    Selector sel;
    if (ctx->Count() && ctx->getText().find("*") != std::string::npos) {
        sel.isCountStar = true;
        sel.aggregate = AggregateType::COUNT;
    } else if (ctx->aggregator()) {
        sel.aggregate = parseAggregatorNode(ctx->aggregator());
        if (ctx->column()) {
            sel.column = parseColumnNode(ctx->column());
        }
    } else if (ctx->column()) {
        sel.column = parseColumnNode(ctx->column());
    }
    return sel;
}
std::any SQLStatementVisitor::visitSelectors(SQLParser::SelectorsContext* ctx) {
    std::vector<Selector> selectors;
    if (ctx->getText().find("*") == 0 || ctx->getText() == "*") {
        Selector sel;
        sel.isAllColumns = true;
        selectors.push_back(sel);
        return selectors;
    }
    for (auto selCtx : ctx->selector()) {
        auto result = selCtx->accept(this);
        selectors.push_back(std::any_cast<Selector>(result));
    }
    return selectors;
}
std::any SQLStatementVisitor::visitIdentifiers(SQLParser::IdentifiersContext* ctx) {
    std::vector<std::string> ids;
    for (auto id : ctx->Identifier()) {
        ids.push_back(id->getText());
    }
    return ids;
}
std::any SQLStatementVisitor::visitSet_clause(SQLParser::Set_clauseContext* ctx) {
    std::vector<SetClause> setClauses;
    auto identifiers = ctx->Identifier();
    auto values = ctx->value();
    for (size_t i = 0; i < identifiers.size() && i < values.size(); i++) {
        SetClause sc;
        sc.column = identifiers[i]->getText();
        sc.value = parseValueNode(values[i]);
        setClauses.push_back(sc);
    }
    return setClauses;
}
std::any SQLStatementVisitor::visitType_(SQLParser::Type_Context* ctx) {
    return ctx->getText();
}

