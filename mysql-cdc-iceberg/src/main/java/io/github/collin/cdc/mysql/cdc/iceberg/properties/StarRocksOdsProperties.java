package io.github.collin.cdc.mysql.cdc.iceberg.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class StarRocksOdsProperties extends AbstractOdsProperties {

    /**
     * StarRocks配置
     */
    private StarRocksProperties starRocks;
    /**
     * 是否开启清空表
     */
    private Boolean openCleanTable;

}