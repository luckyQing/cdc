package io.github.collin.cdc.mysql.cdc.iceberg.dto.cache;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class DdlDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * ddl语句
     */
    private String ddl;
    /**
     * 源数据库
     */
    private String sourceDbName;
    /**
     * 源表名
     */
    private String sourceTableName;
    /**
     * 目标数据库
     */
    private String targetDbName;
    /**
     * 目标表名
     */
    private String targetTableName;
    /**
     * 收到ddl的时间戳
     */
    private Long millis;

}