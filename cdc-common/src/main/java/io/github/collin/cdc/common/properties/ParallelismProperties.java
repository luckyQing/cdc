package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 并发数配置
 *
 * @author collin
 * @date 2023-05-18
 */
@Getter
@Setter
@ToString
public class ParallelismProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 执行并发数
     */
    private int execution = 1;
    /**
     * 写并发数
     */
    private int write = 1;

}
