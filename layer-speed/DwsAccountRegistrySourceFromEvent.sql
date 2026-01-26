-- ============================================================
-- DwsAccountRegistrySourceFromEvent FlinkSQL - 实时消费Kafka写入ClickHouse
-- 数据源：
--   1. app_event_account - 账号埋点（account_register注册事件）
-- 写入：dws_account_registry_source_from_event_di
--
-- 处理语义：
--   从埋点事件中提取注册来源信息，按多维度聚合统计新注册账号
--   维度来源于埋点自带的app_context、device_info、properties字段
--   使用CUMULATE窗口实现天内累加，每分钟输出当天累计值
-- ============================================================

-- 设置时区，确保CUMULATE窗口从北京时间00:00开始
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 1. 创建 Kafka Source 表 - 账号埋点
CREATE TABLE kafka_event_account (
    event_id            STRING,
    trace_id            STRING,
    session_id          STRING,
    mid                 BIGINT,
    client_ts           BIGINT,
    server_ts           BIGINT,
    url_path            STRING,
    referer             STRING,
    ua                  STRING,
    ip                  STRING,
    device_info         ROW<
        device_id STRING,
        idfa STRING,
        oaid STRING,
        android_id STRING,
        brand STRING,
        model STRING,
        os STRING,
        os_version STRING,
        screen_width INT,
        screen_height INT,
        carrier STRING,
        network STRING,
        battery_level INT,
        is_charging BOOLEAN,
        timezone STRING,
        lang STRING,
        dpi INT
    >,
    app_context         ROW<
        app_version STRING,
        build_number INT,
        channel STRING,
        platform STRING,
        ab_test_groups ARRAY<STRING>,
        push_enabled BOOLEAN,
        location_enabled BOOLEAN,
        sdk_version STRING
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
    'properties.group.id' = 'flink-dws-account-registry-source-from-event',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 ClickHouse Sink 表 - 每日新注册账号来源分析表（基于埋点事件）
CREATE TABLE clickhouse_dws_account_registry_source_from_event (
    -- 来源维度（从埋点提取）
    platform                    STRING,
    channel                     STRING,
    register_type               STRING,
    source_page                 STRING,
    os                          STRING,
    brand                       STRING,
    app_version                 STRING,
    network                     STRING,

    -- 聚合指标
    new_account_cnt             BIGINT,
    new_device_cnt              BIGINT,

    -- ETL信息
    dw_create_time              TIMESTAMP(3),
    dt                          STRING,
    PRIMARY KEY (platform, channel, register_type, source_page, os, brand, app_version, network, dt) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '${clickhouse.url}',
    'table-name' = 'dws_account_registry_source_from_event_di',
    'username' = '${clickhouse.username}',
    'password' = '${clickhouse.password}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '10s'
);

-- ============================================================
-- 3. 实时聚合新注册账号，按来源维度汇总
-- 使用CUMULATE窗口，每1分钟输出当天从00:00到当前的累计值
-- 处理语义：
--   - 统计新注册账号（account_register事件）
--   - 按埋点自带的来源信息聚合
-- ============================================================
INSERT INTO clickhouse_dws_account_registry_source_from_event
SELECT
    -- 来源维度（从埋点提取）
    COALESCE(app_context.platform, 'unknown')               AS platform,
    COALESCE(properties.channel, app_context.channel, 'unknown') AS channel,
    COALESCE(properties.register_type, 'unknown')           AS register_type,
    COALESCE(properties.source_page, 'unknown')             AS source_page,
    COALESCE(device_info.os, 'unknown')                     AS os,
    COALESCE(device_info.brand, 'unknown')                  AS brand,
    COALESCE(app_context.app_version, 'unknown')            AS app_version,
    COALESCE(device_info.network, 'unknown')                AS network,

    -- 聚合指标（当天累计值）
    COUNT(*)                                                AS new_account_cnt,
    COUNT(DISTINCT device_info.device_id)                   AS new_device_cnt,

    -- ETL信息
    CURRENT_TIMESTAMP                                       AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd')                   AS dt

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
GROUP BY
    window_start,
    window_end,
    COALESCE(app_context.platform, 'unknown'),
    COALESCE(properties.channel, app_context.channel, 'unknown'),
    COALESCE(properties.register_type, 'unknown'),
    COALESCE(properties.source_page, 'unknown'),
    COALESCE(device_info.os, 'unknown'),
    COALESCE(device_info.brand, 'unknown'),
    COALESCE(app_context.app_version, 'unknown'),
    COALESCE(device_info.network, 'unknown');