package io.github.collin.cdc.ods.properties;

import io.github.collin.cdc.common.properties.RobotProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class MonitorProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * ddl接收监控
     */
    private RobotProperties ddl = new RobotProperties();
    /**
     * ddl同步
     */
    private RobotProperties ddlSync = new RobotProperties();
    /**
     * 表数据物理删除接收监控
     */
    private RobotProperties delete = new RobotProperties();

}