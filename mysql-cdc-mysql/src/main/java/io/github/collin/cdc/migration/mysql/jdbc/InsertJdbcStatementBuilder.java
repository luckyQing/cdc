package io.github.collin.cdc.migration.mysql.jdbc;

import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;

import java.util.ArrayList;
import java.util.List;

/**
 * insert sql赋值
 *
 * @author collin
 * @date 2023-07-09
 */
public class InsertJdbcStatementBuilder extends AbstractJdbcStatementBuilder {

    public InsertJdbcStatementBuilder(List<ColumnMetaDataDTO> columnMetaDatas) {
        super(columnMetaDatas);

        this.fields = new ArrayList<>(columnMetaDatas.size());
        for (ColumnMetaDataDTO columnMetaData : columnMetaDatas) {
            fields.add(columnMetaData.getName());
        }
    }

}