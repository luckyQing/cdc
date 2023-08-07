package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * redis配置属性
 *
 * @author collin
 * @date 2023-05-06
 */
@Getter
@Setter
@ToString
public class RedisProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    private String host;

    private int port;

    private String password;

    private int database = 13;

}