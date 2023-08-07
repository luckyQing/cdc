package io.github.collin.cdc.migration.mysql.jdbc;

import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;

import java.util.ArrayList;
import java.util.List;

/**
 * update sql赋值
 *
 * @author collin
 * @date 2023-07-09
 */
public class UpdateJdbcStatementBuilder extends AbstractJdbcStatementBuilder {

    public UpdateJdbcStatementBuilder(List<ColumnMetaDataDTO> columnMetaDatas) {
        super(columnMetaDatas);

        this.fields = new ArrayList<>(columnMetaDatas.size());
        for (ColumnMetaDataDTO columnMetaData : columnMetaDatas) {
            if (columnMetaData.isPrimaryKey()) {
                continue;
            }
            fields.add(columnMetaData.getName());
        }
        for (ColumnMetaDataDTO columnMetaData : columnMetaDatas) {
            if (columnMetaData.isPrimaryKey()) {
                fields.add(columnMetaData.getName());
            }
        }
    }

}