package io.github.collin.cdc.common.common.adapter;

import io.github.collin.cdc.common.properties.RedisProperties;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

/**
 * redis工具类
 *
 * @author collin
 * @date 2023-05-06
 */
public class RedisAdapter {

    private RedissonClient redissonClient = null;

    public RedisAdapter(final RedisProperties redisProperties) {
        Config config = new Config();
        config.setCodec(new StringCodec());
        SingleServerConfig singleServerConfig = config.useSingleServer();
        singleServerConfig.setAddress(String.format("redis://%s:%s", redisProperties.getHost(), redisProperties.getPort()))
                .setDatabase(redisProperties.getDatabase());
        if (redisProperties.getPassword() != null) {
            singleServerConfig.setPassword(redisProperties.getPassword());
        }

        this.redissonClient = Redisson.create(config);
    }

    /**
     * 获取redis客户端实例
     *
     * @return
     */
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

}