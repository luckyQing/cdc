**备注**：
- 批任务不能带参数“--detached”
- 并行度不可改小，否则savepoint后不可恢复（报错）
- checkpoint间隔时间不可过大，时间间隔越大，内存要求越大
- 数据库连接池连接数不可过小，否则checkpoint有时会超时


## 全量
```
/usr/local/flink-1.17.1/bin/flink run \
-Djobmanager.memory.process.size=2048m \
-Djobmanager.memory.jvm-overhead.min=256m \
-Djobmanager.memory.jvm-overhead.max=256m \
-Dtaskmanager.memory.process.size=18432m \
-Dtaskmanager.memory.managed.size=0m \
-Dtaskmanager.memory.network.min=128m \
-Dtaskmanager.memory.network.max=128m \
-Dtaskmanager.memory.jvm-metaspace.size=256m \
-Dtaskmanager.memory.jvm-overhead.min=256m \
-Dtaskmanager.memory.jvm-overhead.max=256m \
-Dyarn.application.name='sync business_192.168.125.4 to iceberg(ods)' \
-Dstate.checkpoints.num-retained=2 \
-t yarn-per-job --detached \
-c io.github.collin.cdc.mysql.cdc.ods.App /data/pkg/mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
prod/application-business-prod.yaml
```
## 增量
```
/usr/local/flink-1.17.1/bin/flink run \
-Djobmanager.memory.process.size=2048m \
-Djobmanager.memory.jvm-overhead.min=256m \
-Djobmanager.memory.jvm-overhead.max=256m \
-Djobmanager.memory.jvm-metaspace.size=256m \
-Dtaskmanager.memory.process.size=20480m \
-Dtaskmanager.memory.managed.size=0m \
-Dtaskmanager.memory.network.min=64m \
-Dtaskmanager.memory.network.max=64m \
-Dtaskmanager.memory.jvm-metaspace.size=256m \
-Dtaskmanager.memory.jvm-overhead.min=256m \
-Dtaskmanager.memory.jvm-overhead.max=256m \
-Dyarn.application.name='sync business_192.168.125.4 to iceberg(ods)' \
-Dstate.checkpoints.num-retained=2 \
-t yarn-per-job --detached \
-c io.github.collin.cdc.mysql.cdc.ods.App /data/pkg/mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
prod/application-business-prod.yaml
```

# mysql to mysql
```
/usr/local/flink-1.17.1/bin/flink run \
-Djobmanager.memory.process.size=26624m \
-Djobmanager.memory.jvm-metaspace.size=128m \
-Djobmanager.memory.jvm-overhead.min=128m \
-Djobmanager.memory.jvm-overhead.max=128m \
-Dtaskmanager.memory.process.size=14336m \
-Dtaskmanager.memory.managed.size=0m \
-Dtaskmanager.memory.network.min=32m \
-Dtaskmanager.memory.network.max=32m \
-Dtaskmanager.memory.jvm-metaspace.size=128m \
-Dtaskmanager.memory.jvm-overhead.min=256m \
-Dtaskmanager.memory.jvm-overhead.max=256m \
-Dyarn.application.name='sync risk mysql to mysql' \
-Dstate.checkpoints.num-retained=2 \
-t yarn-per-job --detached \
-c io.github.collin.cdc.mysql.cdc.mysql.App /data/pkg/mysql-cdc-mysql-1.0.0-SNAPSHOT.jar \
migration/31/application-10019.yaml
```

# dwd sync data
```
/usr/local/flink-1.17.1/bin/flink run \
-Dyarn.application.queue=root.users.root \
-Djobmanager.memory.process.size=4096m \
-Djobmanager.memory.jvm-overhead.min=512m \
-Djobmanager.memory.jvm-overhead.max=512m \
-Dtaskmanager.memory.process.size=16384m \
-Dtaskmanager.memory.managed.size=2048m \
-Dtaskmanager.memory.network.min=128m \
-Dtaskmanager.memory.network.max=128m \
-Dtaskmanager.memory.jvm-metaspace.size=150m \
-Dyarn.application.name='dwd sync data' \
-Dtaskmanager.memory.jvm-overhead.min=512m \
-Dtaskmanager.memory.jvm-overhead.max=512m \
-t yarn-per-job \
-c io.github.collin.cdc.mysql.cdc.ods.SyncBatchJobApp /data/pkg/dwd-1.0.0-SNAPSHOT.jar
```



# 合并小文件（nn上执行）
```
/usr/local/flink-1.17.1/bin/flink run\
    -Dyarn.application.name=Iceberg-Optimizer-1\
    -Djobmanager.memory.process.size=2048m\
    -Dtaskmanager.memory.process.size=8192m\
    -Dtaskmanager.memory.managed.size=32m\
    -Dtaskmanager.memory.network.min=16m\
    -Dtaskmanager.memory.network.max=16m\
    -Dtaskmanager.memory.jvm-overhead.min=256m\
    -Dtaskmanager.memory.jvm-overhead.max=256m\
    -Dtaskmanager.memory.jvm-metaspace.size=256m\
    -c com.netease.arctic.optimizer.flink.FlinkOptimizer\
    /usr/local/amoro-0.6.0/plugin/optimize/OptimizeJob.jar\
    -a thrift://dn-103:1261\
    -g iceberg_ex\
    -p 4 -m 8192 -hb 10000
```