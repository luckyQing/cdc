package io.github.collin.cdc.mysql.cdc.common.enums;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 表同步类型（1、数据库整个表全部同步；2、只同步指定的表；3、除了指定的表，全部同步）
 *
 * @author collin
 * @date 2019-07-14
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum SyncType {

    /**
     * 数据库整个表全部同步
     */
    ALL(1),
    /**
     * 只同步指定的表
     */
    INCLUDE(2),
    /**
     * 除了指定的表，全部同步
     */
    EXCLUDE(3);

    /**
     * 生成类型
     */
    private Integer type;

}