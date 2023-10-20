package io.github.collin.cdc.migration.mysql.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class DatasourceCdcProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * serverId开始值
     */
    private int startServerId = 7001;
    private Long startupTimestampMillis;
    /**
     * 源数据源配置
     */
    private DatasourceProperties source = new DatasourceProperties();
    /**
     * 目标数据源配置
     */
    private DatasourceProperties target = new DatasourceProperties();

    /**
     * 配置详情<数据库名, 详细配置>
     */
    private Map<String, DatasourceRuleProperties> details = new HashMap<>();

    /**
     * 获取其中一个数据库
     *
     * @return
     */
    public String getOneDatabaseName() {
        return details.keySet().stream().findFirst().get();
    }

}