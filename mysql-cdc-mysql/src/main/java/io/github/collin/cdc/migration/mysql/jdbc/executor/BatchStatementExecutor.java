package io.github.collin.cdc.migration.mysql.jdbc.executor;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class BatchStatementExecutor<T, V> implements JdbcBatchStatementExecutor<T> {

    private final String sql;
    private final JdbcStatementBuilder<V> parameterSetter;
    private final Function<T, V> valueTransformer;

    private transient AtomicInteger counter;
    private transient PreparedStatement st;

    public BatchStatementExecutor(String sql, JdbcStatementBuilder<V> statementBuilder, Function<T, V> valueTransformer) {
        this.sql = sql;
        this.parameterSetter = statementBuilder;
        this.valueTransformer = valueTransformer;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.counter = new AtomicInteger(0);
        this.st = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(T record) throws SQLException {
        synchronized (st) {
            parameterSetter.accept(st, valueTransformer.apply(record));
            st.addBatch();
            counter.incrementAndGet();
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        synchronized (st) {
            if (counter.get() > 0) {
                st.executeBatch();
                st.clearBatch();
                counter.set(0);
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }
    }
}