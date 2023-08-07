package io.github.collin.cdc.migration.mysql.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 分表策略
 * <p>因授信单最后三位尾号由uid%1000计算而来，故分表数量最多支持[0,999]</p>
 *
 * @author collin
 * @date 2023-07-20
 */
@Getter
@AllArgsConstructor
public enum TableShardingType {

    /**
     * 1个表
     */
    ONE(1, 1),
    /**
     * 10表
     */
    TEN(2, 10),
    /**
     * 100个表
     */
    HUNDRED(3, 100),
    /**
     * 1000个表
     */
    THOUSAND(4, 1000);

    /**
     * 表分片类型
     */
    private int type;
    /**
     * 表数量
     */
    private int tableCount;

}