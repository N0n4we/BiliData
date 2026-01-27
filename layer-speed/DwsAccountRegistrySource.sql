-- ============================================================
-- DwsAccountRegistrySource FlinkSQL - 实时消费Kafka写入ClickHouse
-- 数据源：
--   1. app_event_account - 账号埋点（account_register注册事件）
-- 写入：dws_account_registry_source_di
--
-- 处理语义：
--   统计新注册账号，按多维度聚合
--   注：实时层不lookup维度表，维度字段填null，由离线层T+1覆盖补全
--   使用CUMULATE窗口实现天内累加，每分钟输出当天累计值
-- ============================================================

-- 设置时区，确保CUMULATE窗口从北京时间00:00开始
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 1. 创建 Kafka Source 表 - 账号埋点
CREATE TABLE kafka_event_account (
    event_id            STRING,
    mid                 BIGINT,
    server_ts           BIGINT,
    device_info         ROW<
        device_id STRING,
        brand STRING,
        model STRING,
        os STRING,
        os_version STRING
    >,
    app_context         ROW<
        app_version STRING,
        channel STRING,
        platform STRING
    >,
    properties          ROW<
        event_type STRING,
        source_page STRING,
        register_type STRING,
        invite_code STRING,
        channel STRING
    >,
    proc_time AS PROCTIME(),
    event_time AS TO_TIMESTAMP_LTZ(server_ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_account',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dws-account-registry-source',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 ClickHouse Sink 表 - 每日新注册账号来源分析表
-- 字段与batch层/ClickHouse DDL保持一致，维度字段填null
CREATE TABLE clickhouse_dws_account_registry_source (
    -- 来源维度（实时层填null，由离线层补全）
    sex                     STRING,
    `level`                 INT,
    age                     INT,
    birth_year              INT,
    vip_type                INT,
    vip_type_name           STRING,
    `status`                INT,
    status_name             STRING,
    official_type           INT,
    is_official             BOOLEAN,
    theme                   STRING,
    primary_tag             STRING,

    -- 聚合指标
    new_account_cnt         BIGINT,

    -- ETL信息
    dw_create_time          TIMESTAMP(3),
    dt                      STRING,
    PRIMARY KEY (sex, `level`, age, birth_year, vip_type, `status`, official_type, theme, primary_tag, dt) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/default',
    'table-name' = 'dws_account_registry_source_di',
    'username' = 'default',
    'password' = '${clickhouse.password}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '10s'
);

-- ============================================================
-- 3. 实时聚合新注册账号
-- 使用CUMULATE窗口，每1分钟输出当天从00:00到当前的累计值
-- 处理语义：
--   - 统计新注册账号（account_register事件）
--   - 维度字段填null，由离线层T+1覆盖补全
-- ============================================================
INSERT INTO clickhouse_dws_account_registry_source
SELECT
    -- 来源维度（填null，由离线层补全）
    CAST(NULL AS STRING)            AS sex,
    CAST(NULL AS INT)               AS `level`,
    CAST(NULL AS INT)               AS age,
    CAST(NULL AS INT)               AS birth_year,
    CAST(NULL AS INT)               AS vip_type,
    CAST(NULL AS STRING)            AS vip_type_name,
    CAST(NULL AS INT)               AS `status`,
    CAST(NULL AS STRING)            AS status_name,
    CAST(NULL AS INT)               AS official_type,
    CAST(NULL AS BOOLEAN)           AS is_official,
    CAST(NULL AS STRING)            AS theme,
    CAST(NULL AS STRING)            AS primary_tag,

    -- 聚合指标（当天累计值）
    COUNT(*)                        AS new_account_cnt,

    -- ETL信息
    CURRENT_TIMESTAMP               AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd') AS dt

FROM TABLE(
    CUMULATE(
        TABLE kafka_event_account,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,    -- 步长：每1分钟输出一次
        INTERVAL '1' DAY        -- 最大窗口：1天（从00:00开始）
    )
)
WHERE event_id = 'account_register'
  AND mid IS NOT NULL
GROUP BY window_start, window_end;
