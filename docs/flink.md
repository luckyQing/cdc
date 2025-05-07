# savepoint
```
/usr/local/flink-1.17.1/bin/flink stop 19a74c8d3e2c86d3e77dea3adafc6f53 hdfs://nameservice/flink/savepoints yarn-per-job -Dyarn.application.id=application_1686017206494_0018
```

# kill application
```
yarn application -kill application_1684223940566_0010
```

# 查看任务列表
yarn application -list|grep ods

# 查看任务日志
yarn logs -applicationId application_1729671481632_0682

# 执行测试用例时，本地访问webui界面
http://localhost:8081