package io.github.collin.cdc.migration.mysql.util;

import io.github.collin.cdc.migration.mysql.constants.SqlConstants;
import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.migration.mysql.jdbc.DecorateJdbcOutputFormat;
import io.github.collin.cdc.migration.mysql.jdbc.DeleteJdbcStatementBuilder;
import io.github.collin.cdc.migration.mysql.jdbc.InsertJdbcStatementBuilder;
import io.github.collin.cdc.migration.mysql.jdbc.UpdateJdbcStatementBuilder;
import io.github.collin.cdc.migration.mysql.jdbc.executor.BatchStatementExecutor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;

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

    /**
     * 构建插入sql
     *
     * @param dbName
     * @param tableName
     * @param columnMetaDatas
     * @return
     */
    public static String buildInsertSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        return String.format(
                "INSERT DELAYED INTO %s.%s(%s)VALUES(%s) ON DUPLICATE KEY UPDATE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().map(x -> x.getName()).collect(Collectors.joining(",")),
                columnMetaDatas.stream().map(x -> "?").collect(Collectors.joining(",")),
                columnMetaDatas.stream().filter(x -> !x.isPrimaryKey())
                        .map(x -> String.format("%s=VALUES(%s)", x.getName(), x.getName()))
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
    public static String buildUpdateSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        return String.format(
                "UPDATE %s.%s SET %s WHERE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().filter(x -> !x.isPrimaryKey()
                                && !SqlConstants.FIELD_DB_INDEX.equals(x.getName())
                                && !SqlConstants.FIELD_TABLE_INDEX.equals(x.getName()))
                        .map(x -> String.format("%s=?", x.getName()))
                        .collect(Collectors.joining(",")),
                columnMetaDatas.stream().filter(x -> x.isPrimaryKey())
                        .map(x -> String.format("%s=?", x.getName()))
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
    public static String buildDeleteSql(String dbName, String tableName, List<ColumnMetaDataDTO> columnMetaDatas) {
        return String.format(
                "DELETE FROM %s.%s WHERE %s",
                dbName,
                tableName,
                columnMetaDatas.stream().filter(x -> x.isPrimaryKey())
                        .map(x -> String.format("%s=?", x.getName()))
                        .collect(Collectors.joining(" AND ")));
    }

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildInsertOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String insertSql = JdbcUtil.buildInsertSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("insertOurputFormat==>" + insertSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> insertOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> new BatchStatementExecutor(insertSql, new InsertJdbcStatementBuilder(columnMetaDatas), Function.identity()),
                JdbcOutputFormat.RecordExtractor.identity());
        insertOurputFormat.setRuntimeContext(ctx);
        insertOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return insertOurputFormat;
    }

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildUpdateOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String updateSql = JdbcUtil.buildUpdateSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("updateOurputFormat==>" + updateSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> updateOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> new BatchStatementExecutor(updateSql, new UpdateJdbcStatementBuilder(columnMetaDatas), Function.identity()),
                JdbcOutputFormat.RecordExtractor.identity());
        updateOurputFormat.setRuntimeContext(ctx);
        updateOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return updateOurputFormat;
    }

    public static JdbcOutputFormat<Map<String, Object>, ?, ?> buildDeleteOurputFormats(RuntimeContext ctx, List<ColumnMetaDataDTO> columnMetaDatas, JdbcExecutionOptions jdbcExecutionOptions,
                                                                                       SimpleJdbcConnectionProvider connectionProvider, String targetDbName, String targetDbTable) throws IOException {
        String deleteSql = JdbcUtil.buildDeleteSql(targetDbName, targetDbTable, columnMetaDatas);
        System.out.println("deleteOurputFormat==>" + deleteSql);
        JdbcOutputFormat<Map<String, Object>, ?, ?> deleteOurputFormat = new DecorateJdbcOutputFormat(connectionProvider,
                jdbcExecutionOptions,
                (context) -> new BatchStatementExecutor(deleteSql, new DeleteJdbcStatementBuilder(columnMetaDatas), Function.identity()),
                JdbcOutputFormat.RecordExtractor.identity());
        deleteOurputFormat.setRuntimeContext(ctx);
        deleteOurputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

        return deleteOurputFormat;
    }

}