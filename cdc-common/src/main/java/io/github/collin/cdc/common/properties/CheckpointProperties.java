package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * checkpoint配置
 *
 * @author collin
 * @date 2023-05-0=10
 */
@Getter
@Setter
@ToString
public class CheckpointProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * checkpoint时间间隔（单位：毫秒）
     */
    private long checkpointInterval = 5000L;
    /**
     * 两次checkpoint之间的最小间隔时间（单位：毫秒）
     */
    private long minPauseBetweenCheckpoints = 5000L;
    /**
     * checkpoint超时时间（单位：毫秒）
     */
    private long checkpointTimeout = 3 * 60 * 1000L;
    /**
     * checkpoint可容忍的最大失败次数
     */
    private int tolerableCheckpointFailureNumber = 8;
    /**
     * 最大并发数
     */
    private int maxConcurrentCheckpoints = 1;

}