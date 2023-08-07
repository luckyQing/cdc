package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * hdfs配置
 *
 * @author collin
 * @date 2023-05-08
 */
@Getter
@Setter
@ToString
public class HdfsProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    private String warehouse;
    private String uri;
    /**
     * cache目录
     */
    private String cacheDir;

    public String buildCatalog() {
        return String.format("CREATE CATALOG hive_iceberg WITH ('clients'='5','type'='iceberg','catalog-type'='hive','property-version'='1','uri'='%s','warehouse'='%s')", uri, warehouse);
    }

}