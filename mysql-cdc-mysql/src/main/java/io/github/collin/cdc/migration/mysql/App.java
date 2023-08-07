package io.github.collin.cdc.migration.mysql;

import io.github.collin.cdc.common.enums.YamlEnv;
import io.github.collin.cdc.migration.mysql.cdc.MigrationHandler;
/**
 * cdc 入口类
 *
 * @author collin
 * @date 2023-07-20
 */
public class App {

    /**
     * 任务启动入口方法
     * <pre>
     *      /usr/local/flink-1.17.1/bin/flink run \
     *      -Djobmanager.memory.process.size=26624m \
     *      -Djobmanager.memory.jvm-metaspace.size=128m \
     *      -Djobmanager.memory.jvm-overhead.min=128m \
     *      -Djobmanager.memory.jvm-overhead.max=128m \
     *      -Dtaskmanager.memory.process.size=14336m \
     *      -Dtaskmanager.memory.managed.size=0m \
     *      -Dtaskmanager.memory.network.min=128m \
     *      -Dtaskmanager.memory.network.max=128m \
     *      -Dtaskmanager.memory.jvm-metaspace.size=128m \
     *      -Dtaskmanager.memory.jvm-overhead.min=256m \
     *      -Dtaskmanager.memory.jvm-overhead.max=256m \
     *      -Dyarn.application.name='sync risk mysql to mysql' \
     *      -Dstate.checkpoints.num-retained=3 \
     *      -t yarn-per-job --detached \
     *      -c io.github.collin.cdc.migration.mysql.App /data/pkg/mysql-cdc-mysql-1.0.0-SNAPSHOT.jar \
     *      31/application-10000.yaml
     * </pre>
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String yamlPath = null;
        if (args == null || args.length == 0) {
            yamlPath = YamlEnv.PROD.getFileName();
        } else {
            yamlPath = args[0];
        }
        System.out.println("yamlPath=" + yamlPath);

        new MigrationHandler(yamlPath).execute();
    }

}