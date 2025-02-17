package io.github.collin.cdc.mysql.cdc.ods.enums;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 分区类型
 *
 * @author collin
 * @date 2023-11-03
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum PartitionType {

    /**
     * hash分区
     */
    HASH(null),
    /**
     * 按天分区
     */
    DAY("day"),
    /**
     * 按月分区
     */
    MONTH("month"),
    /**
     * 按年分区
     */
    YEAR("year");

    private String value;

}