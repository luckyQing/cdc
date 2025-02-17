# sync business_192.168.125.4 to SR(ods)
```
/home/shuju/flink-1.17.1/bin/flink run \
-Djobmanager.memory.process.size=8192m \
-Djobmanager.memory.jvm-overhead.min=256m \
-Djobmanager.memory.jvm-overhead.max=256m \
-Dtaskmanager.memory.process.size=30720m \
-Dtaskmanager.memory.managed.size=0m \
-Dtaskmanager.memory.network.min=64m \
-Dtaskmanager.memory.network.max=64m \
-Dtaskmanager.memory.jvm-metaspace.size=256m \
-Dtaskmanager.memory.jvm-overhead.min=256m \
-Dtaskmanager.memory.jvm-overhead.max=256m \
-Dyarn.application.name='sync business_192.168.125.4 to SR(ods)' \
-Dstate.checkpoints.num-retained=2 \
-t yarn-per-job --detached \
-c io.github.collin.cdc.mysql.cdc.ods.App /home/shuju/collin/pkg/mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
starrocks/prod/application-business-prod.yaml
```