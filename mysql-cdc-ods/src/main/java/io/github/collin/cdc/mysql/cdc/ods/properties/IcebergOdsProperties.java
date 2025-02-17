package io.github.collin.cdc.mysql.cdc.ods.properties;

import io.github.collin.cdc.common.properties.HdfsProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class IcebergOdsProperties extends AbstractOdsProperties {

    /**
     * hdfs配置
     */
    private HdfsProperties hdfs;

}