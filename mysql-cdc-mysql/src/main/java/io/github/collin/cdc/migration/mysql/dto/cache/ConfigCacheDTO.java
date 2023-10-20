package io.github.collin.cdc.migration.mysql.dto.cache;

import io.github.collin.cdc.migration.mysql.dto.TableDTO;
import io.github.collin.cdc.migration.mysql.properties.DatasourceProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * 配置缓存信息
 *
 * @author collin
 * @date 2023-07-20
 */
@Getter
@Setter
@ToString
public class ConfigCacheDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 关系<源数据库名.源表名, 目标表名>
     */
    private Map<String, TableDTO> tableRelations;
    /**
     * 目标数据源配置
     */
    private DatasourceProperties targetDatasource;

}