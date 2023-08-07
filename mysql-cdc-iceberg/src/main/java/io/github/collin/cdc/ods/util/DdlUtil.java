package io.github.collin.cdc.ods.util;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.github.collin.cdc.common.util.JacksonUtil;
import io.github.collin.cdc.ods.constants.DbConstants;
import io.github.collin.cdc.ods.enums.MysqlTypeMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;

import java.util.*;

/**
 * ddl工具类
 *
 * @author collin
 * @date 2023-04-11
 */
@Slf4j
public class DdlUtil {

    /**
     * 将ddl转化为arctic sql
     *
     * @param sql
     * @param dbName
     * @param tableName
     * @return
     */
    public static String convertArcticSql(String sql, String dbName, String tableName) {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql, true);
        Set<String> ddls = new LinkedHashSet<>();
        for (SQLStatement statement : statements) {
            if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) statement;
                List<SQLAlterTableItem> sqlAlterTableItems = alterTableStatement.getItems();
                if (sqlAlterTableItems == null || sqlAlterTableItems.size() == 0) {
                    continue;
                }
                for (SQLAlterTableItem sqlAlterTableItem : sqlAlterTableItems) {
                    if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                        SQLAlterTableAddColumn sqlAlterTableAddColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
                        SQLColumnDefinition sqlColumnDefinition = sqlAlterTableAddColumn.getColumns().get(0);
                        String columnName = sqlColumnDefinition.getColumnName();
                        SQLDataType sqlDataType = sqlColumnDefinition.getDataType();
                        SQLCharExpr sqlExpr = (SQLCharExpr) sqlColumnDefinition.getComment();
                        String comment = sqlExpr != null ? sqlExpr.getText() : "";
                        String addColumnName = removeMysqlDelimiter(columnName);

                        String ddl = String.format("alter table %s.%s add columns (%s %s comment '%s'", dbName, tableName, addColumnName, MysqlTypeMapping.of(sqlDataType.getName()), comment);
                        if (sqlAlterTableAddColumn.isFirst()) {
                            ddl += " first)";
                        } else if (sqlAlterTableAddColumn.getAfterColumn() != null) {
                            String afterName = removeMysqlDelimiter(sqlAlterTableAddColumn.getAfterColumn().getSimpleName());
                            ddl += " after " + afterName + ")";
                        } else {
                            ddl += ")";
                        }
                        ddls.add(ddl);
                    } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                        MySqlAlterTableChangeColumn mySqlAlterTableChangeColumn = (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                        String oldColumnName = removeMysqlDelimiter(mySqlAlterTableChangeColumn.getColumnName().getSimpleName());
                        SQLColumnDefinition sqlColumnDefinition = mySqlAlterTableChangeColumn.getNewColumnDefinition();
                        String newColumnName = removeMysqlDelimiter(sqlColumnDefinition.getColumnName());
                        SQLCharExpr sqlExpr = (SQLCharExpr) sqlColumnDefinition.getComment();
                        String comment = sqlExpr != null ? sqlExpr.getText() : null;

                        // 修改列名
                        if (!oldColumnName.equals(newColumnName)) {
                            ddls.add(String.format("alter table %s.%s rename column %s to %s", dbName, tableName, oldColumnName, newColumnName));
                        }
                        // 修改位置
                        if (mySqlAlterTableChangeColumn.isFirst()) {
                            ddls.add(String.format("alter table %s.%s alter column %s first", dbName, tableName, newColumnName));
                        } else {
                            SQLName afterColumn = mySqlAlterTableChangeColumn.getAfterColumn();
                            if (afterColumn != null) {
                                ddls.add(String.format("alter table %s.%s alter column %s after %s", dbName, tableName, newColumnName, removeMysqlDelimiter(afterColumn.getSimpleName())));
                            }
                        }
                        if (comment != null) {
                            ddls.add(String.format("alter table %s.%s alter column %s comment '%s'", dbName, tableName, newColumnName, comment));
                        }
                    } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem sqlAlterTableDropColumnItem = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                        SQLName sqlName = sqlAlterTableDropColumnItem.getColumns().get(0);
                        ddls.add(String.format("alter table %s.%s drop column %s;", dbName, tableName, removeMysqlDelimiter(sqlName.getSimpleName())));
                    } else {
                        log.warn("ddl[{}], skip", sql);
                    }
                }
            } else {
                log.warn("Non-modified structure ddl[{}], skip", sql);
            }
        }

        if (ddls.isEmpty()) {
            return StringUtils.EMPTY;
        }

        return StringUtils.join(ddls, ";\\n");
    }

    /**
     * 同步ddl
     *
     * @param sql
     * @param table
     * @return
     */
    public static boolean syncAlterSql(String sql, Table table) {
        log.warn("sql={}", sql);
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql, true);
        UpdateSchema updateSchema = table.updateSchema();
        Schema schema = table.schema();
        // <newName, oldName>
        Map<String, String> changeColumns = new HashMap<>();
        boolean needCommit = false;
        for (SQLStatement statement : statements) {
            if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) statement;
                List<SQLAlterTableItem> sqlAlterTableItems = alterTableStatement.getItems();
                if (sqlAlterTableItems == null || sqlAlterTableItems.size() == 0) {
                    continue;
                }
                for (SQLAlterTableItem sqlAlterTableItem : sqlAlterTableItems) {
                    if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                        SQLAlterTableAddColumn sqlAlterTableAddColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
                        SQLColumnDefinition sqlColumnDefinition = sqlAlterTableAddColumn.getColumns().get(0);
                        String columnName = sqlColumnDefinition.getColumnName();
                        SQLDataType sqlDataType = sqlColumnDefinition.getDataType();
                        SQLCharExpr sqlExpr = (SQLCharExpr) sqlColumnDefinition.getComment();
                        String comment = sqlExpr != null ? sqlExpr.getText() : null;
                        String addColumnName = removeMysqlDelimiter(columnName);
                        if (schema.findField(addColumnName) != null) {
                            continue;
                        }

                        updateSchema.addColumn(addColumnName, MysqlTypeMapping.of(sqlDataType.getName()), comment);
                        moveColumn(columnName, sqlAlterTableAddColumn.isFirst(), sqlAlterTableAddColumn.getAfterColumn(), updateSchema, changeColumns);
                        needCommit = true;
                    } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                        MySqlAlterTableChangeColumn mySqlAlterTableChangeColumn = (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                        String oldColumnName = removeMysqlDelimiter(mySqlAlterTableChangeColumn.getColumnName().getSimpleName());
                        SQLColumnDefinition sqlColumnDefinition = mySqlAlterTableChangeColumn.getNewColumnDefinition();
                        String newColumnName = removeMysqlDelimiter(sqlColumnDefinition.getColumnName());
                        SQLDataType sqlDataType = sqlColumnDefinition.getDataType();
                        SQLCharExpr sqlExpr = (SQLCharExpr) sqlColumnDefinition.getComment();
                        String comment = sqlExpr != null ? sqlExpr.getText() : null;

                        changeColumns.put(newColumnName, oldColumnName);

                        updateSchema.updateColumn(oldColumnName, MysqlTypeMapping.of(sqlDataType.getName()).asPrimitiveType(), comment);
                        moveColumn(oldColumnName, mySqlAlterTableChangeColumn.isFirst(), mySqlAlterTableChangeColumn.getAfterColumn(), updateSchema, changeColumns);
                        if (!oldColumnName.equals(newColumnName)) {
                            updateSchema.renameColumn(oldColumnName, newColumnName);
                        }
                        needCommit = true;
                    } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem sqlAlterTableDropColumnItem = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                        SQLName sqlName = sqlAlterTableDropColumnItem.getColumns().get(0);
                        updateSchema.deleteColumn(removeMysqlDelimiter(sqlName.getSimpleName()));
                        needCommit = true;
                    } else {
                        log.warn("ddl[{}], skip", sql);
                    }
                }
            } else {
                log.warn("Non-modified structure ddl[{}], skip", sql);
            }
        }

        if (needCommit) {
            updateSchema.commit();
            table.refresh();
            return true;
        }

        return false;
    }

    public static BinlogOffset convert(String sourceOffset) {
        return BinlogOffset.builder().setOffsetMap(JacksonUtil.parseObject(sourceOffset, Map.class)).build();
    }


    /**
     * 移动字段
     *
     * @param columnName
     * @param isFirst
     * @param afterColumn
     * @param updateSchema
     * @param changeColumns
     */
    private static void moveColumn(String columnName, boolean isFirst, SQLName afterColumn, UpdateSchema updateSchema, Map<String, String> changeColumns) {
        if (isFirst) {
            updateSchema.moveFirst(removeMysqlDelimiter(columnName));
        } else {
            if (afterColumn != null) {
                String afterName = removeMysqlDelimiter(afterColumn.getSimpleName());
                String oldAfterName = changeColumns.get(afterName);
                updateSchema.moveAfter(removeMysqlDelimiter(columnName), oldAfterName != null ? oldAfterName : afterName);
            }
        }
    }

    /**
     * 删除mysql字段分隔符“`”
     *
     * @param columnName
     * @return
     */
    private static String removeMysqlDelimiter(String columnName) {
        return StringUtils.removeStart(StringUtils.removeEnd(columnName, DbConstants.MYSQL_DELIMITER), DbConstants.MYSQL_DELIMITER);
    }

}