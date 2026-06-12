<div align="center">

# Flink CDC - 企业级 MySQL 实时数据同步平台

**基于 Apache Flink CDC 构建的生产级数据同步解决方案，支持 MySQL → Iceberg / StarRocks / MySQL / Kafka 多目标实时同步**

[![Flink](https://img.shields.io/badge/Flink-1.17.1-blue)](https://flink.apache.org)
[![Flink CDC](https://img.shields.io/badge/Flink_CDC-2.4.0-orange)](https://ververica.github.io/flink-cdc-connectors/)
[![Iceberg](https://img.shields.io/badge/Iceberg-1.3.0-green)](https://iceberg.apache.org)
[![Java](https://img.shields.io/badge/Java-8-red)](https://www.oracle.com/java/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

---

## Why This Project?

在大规模数据架构中，数据同步往往是最复杂、最容易出问题的环节。本项目诞生于**真实的生产环境挑战**：

- **海量规模**：100+ 数据库实例、10,000+ 库、1,000,000+ 表的跨云迁移（阿里云 → 腾讯云）
- **实时数仓**：大表数据查询性能瓶颈，需要实时同步到 Iceberg/StarRocks 构建 ODS 层
- **零停机**：支持全量 + 增量无缝切换，业务零感知

> 这不是一个 Demo 项目，而是经过生产环境验证、承载百万级表同步的工业级解决方案。

## Core Features

| 特性 | 说明 |
|------|------|
| **多 Sink 支持** | Iceberg / StarRocks / MySQL / Kafka，一套框架覆盖所有同步场景 |
| **整库同步** | 支持多实例、多库、多表整库实时同步，告别逐表配置 |
| **分库分表** | 原生支持分库分表场景，正则匹配源表、自动合并目标表 |
| **自动建库建表** | 自动从源端读取 Schema，在目标端创建对应的库和表 |
| **DDL 感知** | 实时监听 DDL 变更，自动通知（企业微信/钉钉/飞书 Webhook） |
| **物理删除过滤** | 支持配置排除物理删除的表，避免误删数据扩散 |
| **全量 + 增量** | 支持 `INITIAL`（全量+增量）、`LATEST`（仅增量）等多种启动模式 |
| **Exactly-Once** | 基于 Flink Checkpoint 保证端到端 Exactly-Once 语义 |
| **YAML 驱动** | 所有配置通过 YAML 文件管理，灵活、直观、版本可控 |
| **G1 GC 优化** | 内置 JVM G1 垃圾回收器调优，适合大状态长时间运行 |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      MySQL Source (CDC)                       │
│         Multi-Instance / Multi-Database / Multi-Table         │
└────────────────────────┬─────────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │  Flink  │
                    │ Engine  │
                    └────┬────┘
                         │
          ┌──────────┬───┴───┬──────────┐
          ▼          ▼       ▼          ▼
    ┌──────────┐ ┌──────┐ ┌──────┐ ┌───────┐
    │ Iceberg  │ │Star  │ │MySQL │ │ Kafka │
    │  (ODS)   │ │Rocks │ │      │ │       │
    └──────────┘ └──────┘ └──────┘ └───────┘
```

## Modules

```
cdc
├── cdc-common            # 公共模块：常量、工具类、Redis 适配、Flink 环境构建
├── mysql-cdc-common      # MySQL CDC 公共模块：数据模型、监听器、分库分表属性
├── mysql-cdc-ods         # MySQL → Iceberg / StarRocks（ODS 实时数仓）
└── mysql-cdc-mysql       # MySQL → MySQL（跨云/跨实例数据迁移）
```

### `mysql-cdc-ods` — 实时 ODS 层

通过 Flink CDC 将 MySQL 数据实时同步到数据湖/OLAP 引擎，构建 ODS 层：
- **Iceberg**：适合数据湖场景，配合 Trino/Presto 进行交互式查询
- **StarRocks**：适合实时 OLAP 分析场景，毫秒级响应

### `mysql-cdc-mysql` — 数据库迁移

MySQL 到 MySQL 的实时数据迁移，专为跨云迁移设计：
- 源端多实例**共用连接**，极大节约数据库连接资源
- 自动在目标端创建库表（含分库分表索引处理）
- 支持分库分表聚合（正则匹配 → 合并写入）

## Tech Stack

| 组件 | 版本 | 说明 |
|------|------|------|
| Apache Flink | 1.17.1 | 流处理引擎 |
| Flink CDC | 2.4.0 | MySQL CDC Connector |
| Apache Iceberg | 1.3.0 | 数据湖格式 |
| StarRocks Connector | 1.2.10 | StarRocks Flink Connector |
| Flink Connector JDBC | 3.1.1-1.17 | JDBC Sink |
| Apache Kafka | - | 消息队列（可选） |
| Redis (Redisson) | 3.22.0 | 元数据缓存 & 状态管理 |
| Hadoop | 3.3.3 | HDFS / Hive Metastore |

## Quick Start

### 1. 构建

```bash
mvn clean package -DskipTests
```

### 2. 配置文件

以 MySQL → Iceberg 为例，创建 `application.yaml`：

```yaml
env: PROD
application: ods_business
sinkType: ICEBERGE                          # ICEBERGE | STARROCKS
startupMode: INITIAL                        # INITIAL(全量+增量) | LATEST(仅增量)
targetTimeZone: +08:00
parallelism:
  execution: 4
  write: 1
checkpoint:
  checkpointInterval: 180000
  minPauseBetweenCheckpoints: 180000
  checkpointTimeout: 300000
redis:
  host: 127.0.0.1
  port: 6379
  password: your_password
hdfs:
  warehouse: hdfs://namenode/user/hive/warehouse/
  uri: thrift://namenode:9083
datasources:
  business:
    host: 192.168.1.100
    port: 3306
    username: cdc_reader
    password: your_password
    timeZone: +08:00
    details:
      order_db:
        type: 2                             # 1=全部表 2=仅指定表 3=排除指定表
        tables:
          - t_order
          - t_user
      payment_db:
        type: 3
        tables:
          - temp_log                        # 排除临时表
```

### 3. 提交任务

```bash
# MySQL → Iceberg (YARN Per-Job)
flink run \
  -Djobmanager.memory.process.size=4096m \
  -Dtaskmanager.memory.process.size=18432m \
  -Dtaskmanager.memory.managed.size=0m \
  -Dyarn.application.name='mysql-to-iceberg-ods' \
  -Dstate.checkpoints.num-retained=3 \
  -t yarn-per-job --detached \
  -c io.github.collin.cdc.mysql.cdc.ods.App \
  mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
  iceberg/prod/application.yaml

# MySQL → StarRocks (YARN Per-Job)
flink run \
  -Djobmanager.memory.process.size=8192m \
  -Dtaskmanager.memory.process.size=30720m \
  -Dyarn.application.name='mysql-to-starrocks-ods' \
  -t yarn-per-job --detached \
  -c io.github.collin.cdc.mysql.cdc.ods.App \
  mysql-cdc-ods-1.0.0-SNAPSHOT.jar \
  starrocks/prod/application.yaml

# MySQL → MySQL 迁移 (YARN Per-Job)
flink run \
  -Djobmanager.memory.process.size=26624m \
  -Dtaskmanager.memory.process.size=14336m \
  -Dyarn.application.name='mysql-migration' \
  -t yarn-per-job --detached \
  -c io.github.collin.cdc.mysql.cdc.mysql.App \
  mysql-cdc-mysql-1.0.0-SNAPSHOT.jar \
  migration/application.yaml
```

### 4. 分库分表配置示例

```yaml
datasources:
  sharded_instance:
    details:
      order_db:
        type: 2
        sharding:
          sourceDb: order_db_([0-9][0-9])       # 正则匹配源库
          targetDb: order_db_merged              # 合并到目标库
          tables:
            t_order:
              sourceTable: t_order_[0-9]+        # 正则匹配源表
              shardingType: TEN                  # 分表数量
              nodata: false                      # 是否仅同步结构
```

## Configuration Reference

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `sinkType` | 目标类型：`ICEBERGE` / `STARROCKS` | - |
| `startupMode` | 启动模式：`INITIAL` / `LATEST` | `INITIAL` |
| `env` | 环境：`TEST` / `PROD` | - |
| `parallelism.execution` | Source 并行度 | - |
| `parallelism.write` | Sink Write 并行度 | - |
| `checkpoint.checkpointInterval` | Checkpoint 间隔(ms) | - |
| `datasources.*.details.*.type` | 表选择策略：`1`=全部 / `2`=包含 / `3`=排除 | - |
| `datasources.*.details.*.excludeDeleteTables` | 过滤物理删除事件的表 | - |

## Production Tips

- **Checkpoint 间隔**不宜过大，间隔越大内存需求越高
- **并行度**修改后不可从旧 Savepoint 恢复
- 数据库连接池配置需与 Checkpoint 超时时间匹配
- 批任务提交时不要带 `--detached` 参数
- 建议使用 G1 GC 以获得更稳定的长时间运行表现

## Star History

如果这个项目对你有帮助，请给一个 Star :star: 支持！

## License

[Apache License 2.0](https://opensource.org/licenses/Apache-2.0)