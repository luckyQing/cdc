package io.github.collin.cdc.mysql.cdc.iceberg.dto;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TableDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 目标库名
     */
    private String dbName;
    /**
     * 目标表名
     */
    private String tableName;
    /**
     * 是否分库分表
     */
    private boolean sharding = false;

}