> > 背景：<br/>
> 1、因有上百的数据库实例（上万个库，一百多万个表）需要从阿里云迁移到华为云，故有了mysql-cdc-mysql，从通过flink cdc同步mysql数据库到mysql（支持全量、增量）；<br/>
> 2、因部分表数据量比较大，报表数据查询很慢，故有了mysql-cdc-iceberg，通过flink cdc同步mysql数据到iceberg（ods层），再通过flink sql对iceberg表数据进一步清理成为dwd层等；用户界面层通过trino查询iceberg表数据供使用。

# 一、模块介绍
- cdc-common<br/>
  公共代码封装（常量、工具类等）
- mysql-cdc-iceberg<br/>
  通过flink cdc实时同步mysql数据到iceberg，支持多实例整库同步
- mysql-cdc-mysql<br/>
  通过flink cdc实时同步mysql到mysql，源数据库实例共用数据库连接（节约资源）
# 二、使用的组件
名称 | 版本
---|---
[flink](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/overview/) | 1.17.1
[flink cdc](https://ververica.github.io/flink-cdc-connectors/release-2.4/) | 2.4.0
[iceberg](https://iceberg.apache.org) | 1.3.0
[flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc) | 3.1.1-1.17