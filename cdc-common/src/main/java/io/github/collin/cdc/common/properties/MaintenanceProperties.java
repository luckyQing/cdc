package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * flink-maintenance服务配置
 *
 * @author collin
 * @date 2023-05-08
 */
@Getter
@Setter
@ToString
public class MaintenanceProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 重启flink任务接口全路径地址（JobController#restart）
     */
    private String restartUrl;

}