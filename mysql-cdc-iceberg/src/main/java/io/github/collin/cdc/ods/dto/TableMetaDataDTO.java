package io.github.collin.cdc.ods.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 数据库表元数据信息
 *
 * @author collin
 * @date 2019-07-15
 */
@Getter
@Setter
@ToString
public class TableMetaDataDTO {

    /**
     * 表名
     */
    private String name;
    /**
     * 表备注
     */
    private String comment;

}