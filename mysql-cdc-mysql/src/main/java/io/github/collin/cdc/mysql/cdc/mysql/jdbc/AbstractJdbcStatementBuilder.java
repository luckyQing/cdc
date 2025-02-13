package io.github.collin.cdc.mysql.cdc.mysql.jdbc;

import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class AbstractJdbcStatementBuilder implements JdbcStatementBuilder<Map<String, Object>> {

    /**
     * sql参数字段名
     */
    protected List<String> fields;

    public AbstractJdbcStatementBuilder(List<ColumnMetaDataDTO> columnMetaDatas) {
    }

    @Override
    public void accept(PreparedStatement ps, Map<String, Object> m) throws SQLException {
        for (int i = 0; i < fields.size(); i++) {
            ps.setObject(i + 1, m.get(fields.get(i)));
        }

        // 释放
        m.clear();
    }

}