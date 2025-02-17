package io.github.collin.cdc.mysql.cdc.ods.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * StarRocks配置属性
 *
 * @author collin
 * @date 2024-12-02
 */
@Getter
@Setter
@ToString
public class StarRocksProperties implements Serializable {

    /**
     * jdbc地址
     */
    private String jdbcUrl;
    /**
     * 加载地址
     */
    private String loadUrl;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;

}