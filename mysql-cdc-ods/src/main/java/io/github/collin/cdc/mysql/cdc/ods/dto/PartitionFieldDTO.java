package io.github.collin.cdc.mysql.cdc.ods.dto;

import io.github.collin.cdc.mysql.cdc.ods.enums.PartitionType;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.Date;

@Getter
@Setter
@ToString
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class PartitionFieldDTO {

    /**
     * 字段
     */
    private String name;
    /**
     * 分区类型
     */
    private PartitionType partitionType;
    /**
     * 分桶数
     */
    private int numBuckets;
    /**
     * 按时间分区开始时间
     */
    private Date startTableTime;

}