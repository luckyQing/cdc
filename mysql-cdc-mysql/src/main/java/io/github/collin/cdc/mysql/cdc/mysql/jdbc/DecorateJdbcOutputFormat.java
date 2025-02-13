package io.github.collin.cdc.mysql.cdc.mysql.jdbc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import javax.annotation.Nonnull;

public class DecorateJdbcOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>> extends JdbcOutputFormat<In, JdbcIn, JdbcExec> {

    public DecorateJdbcOutputFormat(@Nonnull JdbcConnectionProvider connectionProvider, @Nonnull JdbcExecutionOptions executionOptions, @Nonnull StatementExecutorFactory<JdbcExec> statementExecutorFactory, @Nonnull RecordExtractor<In, JdbcIn> recordExtractor) {
        super(connectionProvider, executionOptions, statementExecutorFactory, recordExtractor);
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
    }

}