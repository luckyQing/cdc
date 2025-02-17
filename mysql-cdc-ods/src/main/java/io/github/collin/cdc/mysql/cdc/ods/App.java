package io.github.collin.cdc.mysql.cdc.ods;

import io.github.collin.cdc.mysql.cdc.ods.util.CdcUtil;

/**
 * ods 入口类
 *
 * @author collin
 * @date 2023-07-20
 */
public class App {

    /**
     * 任务启动入口方法
     * <pre>
     *     /usr/local/flink-1.17.1/bin/flink run \
     *     -Djobmanager.memory.process.size=4096m \
     *     -Djobmanager.memory.jvm-overhead.min=256m \
     *     -Djobmanager.memory.jvm-overhead.max=256m \
     *     -Dtaskmanager.memory.process.size=18432m \
     *     -Dtaskmanager.memory.managed.size=0m \
     *     -Dtaskmanager.memory.network.min=128m \
     *     -Dtaskmanager.memory.network.max=128m \
     *     -Dtaskmanager.memory.jvm-metaspace.size=256m \
     *     -Dtaskmanager.memory.jvm-overhead.min=256m \
     *     -Dtaskmanager.memory.jvm-overhead.max=256m \
     *     -Dyarn.application.name='sync biz mysql to iceberg(ods)' \
     *     -Dstate.checkpoints.num-retained=3 \
     *     -t yarn-per-job --detached \
     *     -c io.github.collin.cdc.mysql.cdc.ods.App /data/pkg/mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
     *     iceberg/prod/application-biz-test-prod.yaml
     * </pre>
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        CdcUtil.createMySQLSyncDatabase(args);
    }


}