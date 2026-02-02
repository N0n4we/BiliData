SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TEMPORARY TABLE kafka_event_account (
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dws-account-registry-source',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE clickhouse_dws_account_registry_source (
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
    dt                      STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_account_registry_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '60s'
);


INSERT INTO clickhouse_dws_account_registry_source
SELECT
    -- 使用占位符代替 NULL
    'unknown'                       AS sex,
    -1                              AS `level`,
    -1                              AS age,
    -1                              AS birth_year,
    -1                              AS vip_type,
    'unknown'                       AS vip_type_name,
    -1                              AS `status`,
    'unknown'                       AS status_name,
    -1                              AS official_type,
    FALSE                           AS is_official,
    'unknown'                       AS theme,
    'unknown'                       AS primary_tag,

    -- 聚合指标（当天累计值）
    COUNT(*)                        AS new_account_cnt,

    -- ETL信息
    CURRENT_TIMESTAMP               AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd') AS dt

FROM TABLE(
    CUMULATE(
        TABLE kafka_event_account,
        DESCRIPTOR(event_time),
        INTERVAL '60' SECOND,
        INTERVAL '1' DAY
    )
)
WHERE event_id = 'account_register'
  AND mid IS NOT NULL
GROUP BY window_start, window_end;
