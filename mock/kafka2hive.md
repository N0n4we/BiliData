## FlinkSQL 作业

提交后 Flink 会⻓期运行这条 DML，实时执行以下动作：
- 订阅并消费所有符合 app_* 的新旧主题
- 解析字段后把数据落到 …/ods.db/ods_app_di/dt=2026-01-07/topic=app_login/…parquet 这样的 HDFS 目录
- 根据 TBLPROPERTIES 自动向 Hive Metastore 执行 ALTER TABLE ods_app_di ADD IF NOT EXISTS PARTITION (dt='2026-01-07',topic='app_login')

```sql
-- ---------------------------------------------------------
-- 1. 作业级配置
-- ---------------------------------------------------------
-- 开启 Checkpoint，这对流式写入文件系统至关重要，决定了多久生成一次文件
SET 'execution.checkpointing.interval' = '10 min';
SET 'state.backend' = 'hashmap';
-- 遇到脏数据忽略，不让作业失败
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- ---------------------------------------------------------
-- 2. 注册 Hive Catalog
-- ---------------------------------------------------------
CREATE CATALOG hive_prod WITH (
  'type'            = 'hive',
  'default-database'= 'ods',
  'hive-conf-dir'   = '/opt/hive/conf'
);

USE CATALOG hive_prod;
USE ods;

-- ---------------------------------------------------------
-- 3. 定义 Kafka 源表
--    使用 TEMPORARY，因为 Kafka 表不需要持久化到 Hive 元数据中
-- ---------------------------------------------------------
CREATE TEMPORARY TABLE kafka_app_all (
  json_str   STRING,
  event_ts   TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  topic      STRING METADATA FROM 'topic' VIRTUAL,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector'  = 'kafka',
  'topic-pattern' = 'app_.*',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id'          = 'flink_app2hive_prod_01',
  'scan.startup.mode'            = 'latest-offset',
  'scan.topic-partition-discovery.interval' = '1 min',
  'format'                       = 'raw'
);

-- ---------------------------------------------------------
-- 4. 定义 Hive 目标表
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_app_di (
  json_str STRING,
  dt    STRING,
  topic STRING
)
PARTITIONED BY (dt, topic)
WITH (
  -- 指定 Connector 为 Hive
  'connector' = 'hive',
  -- Sink 配置
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay'   = '1 min',
  'sink.partition-commit.policy.kind' = 'metastore,success-file',
  'sink.rollover.size'           = '128 mb',
  'sink.rollover-interval'       = '15 min'
);

-- ---------------------------------------------------------
-- 5. 核心逻辑：启动流任务
-- ---------------------------------------------------------
INSERT INTO ods_app_di
SELECT
  json_str,
  DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt,
  topic
FROM kafka_app_all;
```

```xml
<!-- Kafka Connector -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-sql-connector-kafka</artifactId>
    <version>1.17.2</version>
</dependency>

<!-- Hive Connector -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-sql-connector-hive-3.1.3_2.12</artifactId>
    <version>1.17.2</version>
</dependency>

<!-- Hadoop -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-shaded-hadoop-2-uber</artifactId>
    <version>2.8.3-10.0</version>
</dependency>
```


## HiveSQL

### 建表

```sql
CREATE TABLE ods_app_di (
  json_str STRING
)
PARTITIONED BY (dt STRING, topic STRING)
STORED AS TEXTFILE -- 指定为文本格式
TBLPROPERTIES (
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay'   = '1 min',
  'sink.partition-commit.policy.kind' = 'metastore,success-file' -- 核心：自动写入元数据
);
```

### 验证

```sql
-- 查看已有 Hive 分区
SHOW PARTITIONS ods_app_di;

-- 在 Hive / Presto / Spark SQL 中查询
SELECT COUNT(*) FROM ods.ods_app_di
WHERE dt='2026-01-07' AND topic='app_login';
```
