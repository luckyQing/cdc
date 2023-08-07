package io.github.collin.cdc.migration.mysql.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 表字段元数据信息
 *
 * @author collin
 * @date 2019-07-15
 */
@Getter
@Setter
@ToString
public class ColumnMetaDataDTO {

    /**
     * 表字段名
     */
    private String name;
    /**
     * 是否为主键
     */
    @SuppressWarnings("AlibabaPojoMustUsePrimitiveField")
    private boolean primaryKey;

}