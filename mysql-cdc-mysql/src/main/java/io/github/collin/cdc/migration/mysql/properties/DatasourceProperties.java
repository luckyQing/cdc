package io.github.collin.cdc.migration.mysql.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class DatasourceProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据库用户名
     */
    private String username;
    /**
     * 数据库密码
     */
    private String password;
    /**
     * 数据库主机地址
     */
    private String host;
    /**
     * 数据库端口号
     */
    private int port;
    /**
     * 数据库时区
     */
    private String timeZone = "GMT+8";

}