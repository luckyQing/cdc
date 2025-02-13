package io.github.collin.cdc.mysql.cdc.common.dto;

import com.mysql.cj.MysqlType;
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
     * 数据类型
     */
    private MysqlType mysqlType;
    /**
     * 字段备注
     */
    private String comment;
    /**
     * 字段长度
     */
    private Integer length;
    /**
     * 是否为主键
     */
    private boolean primaryKey;
    /**
     * 是否为唯一索引
     */
    private boolean uniqueKey;

}