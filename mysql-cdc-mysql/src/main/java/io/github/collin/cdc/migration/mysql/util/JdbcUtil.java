package io.github.collin.cdc.migration.mysql.util;

import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.migration.mysql.jdbc.DecorateJdbcOutputFormat;
import io.github.collin.cdc.migration.mysql.jdbc.DeleteJdbcStatementBuilder;
import io.github.collin.cdc.migration.mysql.jdbc.InsertJdbcStatementBuilder;
import io.github.collin.cdc.migration.mysql.jdbc.UpdateJdbcStatementBuilder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * sql模板构建
 *
 * @author collin
 * @date 2023-07-10
 */
public class JdbcUtil {

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildInsertOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String insertSql = buildInsertSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("insertOurputFormat==>" + insertSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> insertOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> JdbcBatchStatementExecutor.simple(insertSql, new InsertJdbcStatementBuilder(columnMetaDatas), Function.identity()), JdbcOutputFormat.RecordExtractor.identity());
        insertOurputFormat.setRuntimeContext(ctx);
        insertOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return insertOurputFormat;
    }

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildUpdateOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String updateSql = buildUpdateSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("updateOurputFormat==>" + updateSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> updateOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> JdbcBatchStatementExecutor.simple(updateSql, new UpdateJdbcStatementBuilder(columnMetaDatas), Function.identity()),
                JdbcOutputFormat.RecordExtractor.identity());
        updateOurputFormat.setRuntimeContext(ctx);
        updateOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return updateOurputFormat;
    }

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildDeleteOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String deleteSql = buildDeleteSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("deleteOurputFormat==>" + deleteSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> deleteOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> JdbcBatchStatementExecutor.simple(deleteSql, new DeleteJdbcStatementBuilder(columnMetaDatas), Function.identity()),
                JdbcOutputFormat.RecordExtractor.identity());
        deleteOurputFormat.setRuntimeContext(ctx);
        deleteOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return deleteOurputFormat;
    }

    public static boolean existPrimaryKey(List<ColumnMetaDataDTO> columnMetaDatas) {
        return columnMetaDatas.stream().filter(x -> x.isPrimaryKey()).count() > 0;
    }

    /**
     * 构建插入sql
     *
     * @param dbName
     * @param tableName
     * @param columnMetaDatas
     * @return
     */
    private static String buildInsertSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        if (existPrimaryKey(columnMetaDatas)) {
            return String.format(
                    "INSERT DELAYED INTO %s.%s(%s)VALUES(%s) ON DUPLICATE KEY UPDATE %s",
                    dbName,
                    tableName,
                    columnMetaDatas.stream().map(x -> escapeColumn(x.getName())).collect(Collectors.joining(",")),
                    columnMetaDatas.stream().map(x -> "?").collect(Collectors.joining(",")),
                    columnMetaDatas.stream().filter(x -> !x.isPrimaryKey())
                            .map(x -> String.format("%s=VALUES(%s)", escapeColumn(x.getName()), escapeColumn(x.getName())))
                            .collect(Collectors.joining(",")));
        }

        return String.format(
                "INSERT DELAYED INTO %s.%s(%s)VALUES(%s) ON DUPLICATE KEY UPDATE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().map(x -> escapeColumn(x.getName())).collect(Collectors.joining(",")),
                columnMetaDatas.stream().map(x -> "?").collect(Collectors.joining(",")),
                columnMetaDatas.stream().filter(x -> !x.isUniqueKey())
                        .map(x -> String.format("%s=VALUES(%s)", escapeColumn(x.getName()), escapeColumn(x.getName())))
                        .collect(Collectors.joining(",")));
    }

    /**
     * 构建更新sql
     *
     * @param dbName
     * @param tableName
     * @param columnMetaDatas
     * @return
     */
    private static String buildUpdateSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        if (existPrimaryKey(columnMetaDatas)) {
            return String.format(
                    "UPDATE %s.%s SET %s WHERE %s",
                    dbName,
                    tableName,
                    columnMetaDatas.stream().filter(x -> !x.isPrimaryKey())
                            .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                            .collect(Collectors.joining(",")),
                    columnMetaDatas.stream().filter(x -> x.isPrimaryKey())
                            .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                            .collect(Collectors.joining(" AND ")));
        }

        return String.format(
                "UPDATE %s.%s SET %s WHERE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().filter(x -> !x.isUniqueKey())
                        .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                        .collect(Collectors.joining(",")),
                columnMetaDatas.stream().filter(x -> x.isUniqueKey())
                        .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                        .collect(Collectors.joining(" AND ")));
    }

    /**
     * 构建删除sql
     *
     * @param dbName
     * @param tableName
     * @param columnMetaDatas
     * @return
     */
    private static String buildDeleteSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        if (existPrimaryKey(columnMetaDatas)) {
            return String.format(
                    "DELETE FROM %s.%s WHERE %s",
                    dbName,
                    tableName,
                    columnMetaDatas.stream().filter(x -> x.isPrimaryKey())
                            .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                            .collect(Collectors.joining(" AND ")));
        }
        return String.format(
                "DELETE FROM %s.%s WHERE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().filter(x -> x.isUniqueKey())
                        .map(x -> String.format("%s=?", escapeColumn(x.getName())))
                        .collect(Collectors.joining(" AND ")));
    }

    /**
     * 表字段转义
     *
     * @param column
     * @return
     */
    private static String escapeColumn(String column) {
        return String.format("`%s`", column);
    }

}