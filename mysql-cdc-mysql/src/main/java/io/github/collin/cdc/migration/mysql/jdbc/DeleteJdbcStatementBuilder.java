package io.github.collin.cdc.migration.mysql.jdbc;

import io.github.collin.cdc.migration.mysql.dto.ColumnMetaDataDTO;

import java.util.List;
import java.util.stream.Collectors;

/**
 * delete sql赋值
 *
 * @author collin
 * @date 2023-07-09
 */
public class DeleteJdbcStatementBuilder extends AbstractJdbcStatementBuilder {

    public DeleteJdbcStatementBuilder(List<ColumnMetaDataDTO> columnMetaDatas) {
        super(columnMetaDatas);

        this.fields = columnMetaDatas.stream()
                .filter(x -> x.isPrimaryKey())
                .map(ColumnMetaDataDTO::getName)
                .collect(Collectors.toList());
    }

}