package io.github.collin.cdc.mysql.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class FlinkDatasourceProperties implements Serializable {

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
     * serverId开始值
     */
    private int startServerId = 6001;
    /**
     * 源数据库时区
     */
    private String timeZone;
    /**
     * 配置详情<数据库名, 详细配置>
     */
    private Map<String, FlinkDatasourceDetailProperties> details = new HashMap<>();

    /**
     * 获取其中一个数据库
     *
     * @return
     */
    public String getOneDatabaseName() {
        return details.keySet().stream().findFirst().get();
    }

}