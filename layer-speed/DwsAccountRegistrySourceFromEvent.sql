SET 'table.local-time-zone' = 'Asia/Shanghai';

DROP TABLE IF EXISTS kafka_event_account;
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dws-account-registry-source-from-event',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

DROP TABLE IF EXISTS clickhouse_dws_account_registry_source_from_event;
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
    dt                          STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_account_registry_source_from_event_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

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

    -- 聚合指标（小时内累计值）
    COUNT(*)                                                AS new_account_cnt,
    COUNT(DISTINCT device_info.device_id)                   AS new_device_cnt,

    -- ETL信息
    CURRENT_TIMESTAMP                                       AS dw_create_time,
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:00:00')        AS dt  -- 小时开始时间

FROM TABLE(
    CUMULATE(
        TABLE kafka_event_account,
        DESCRIPTOR(event_time),
        INTERVAL '15' SECOND,
        INTERVAL '1' HOUR
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
