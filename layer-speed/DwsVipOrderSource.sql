SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TEMPORARY TABLE kafka_event_vip (
    event_id            STRING,
    mid                 BIGINT,
    server_ts           BIGINT,
    order_no            STRING,
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
    url_path            STRING,
    properties          ROW<
        plan_id STRING,
        plan_name STRING,
        plan_price BIGINT,
        plan_duration_days INT,
        original_price BIGINT,
        final_price BIGINT,
        discount_amount BIGINT,
        coupon_id STRING,
        pay_method STRING,
        amount BIGINT,
        pay_duration_sec INT,
        new_vip_expire BIGINT,
        error_code STRING,
        error_msg STRING,
        retry_count INT,
        cancel_stage STRING,
        time_on_pay_page_sec INT,
        `from` STRING,
        current_vip_type INT,
        current_vip_expire BIGINT
    >,
    proc_time AS PROCTIME(),
    event_time AS TO_TIMESTAMP_LTZ(server_ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_vip',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dws-vip-order-source',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE clickhouse_dws_vip_order_source (
    -- 订单维度
    order_status                STRING,
    plan_id                     STRING,
    plan_name                   STRING,
    plan_duration_days          INT,
    pay_method                  STRING,
    platform                    STRING,
    channel                     STRING,
    from_page                   STRING,
    current_vip_type            INT,
    coupon_id                   STRING,
    error_code                  STRING,
    cancel_stage                STRING,

    -- 用户维度（实时层填占位符，由离线层补全）
    sex                         STRING,
    age                         INT,
    birth_year                  INT,
    `level`                     INT,
    official_type               INT,
    is_official                 BOOLEAN,
    primary_tag                 STRING,
    account_age_days            INT,
    follower_cnt                INT,

    -- 聚合指标
    order_cnt                   BIGINT,
    order_user_cnt              BIGINT,
    original_price              DECIMAL(12,2),
    final_price                 DECIMAL(12,2),
    discount_amount             DECIMAL(12,2),
    pay_duration_sec            INT,

    -- ETL信息
    dw_create_time              TIMESTAMP(3),
    dt                          STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_vip_order_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

INSERT INTO clickhouse_dws_vip_order_source
SELECT
    -- ========== 订单维度 ==========
    -- 事件类型映射为订单状态
    CASE event_id
        WHEN 'vip_create_order' THEN 'created'
        WHEN 'vip_pay_start'    THEN 'paying'
        WHEN 'vip_pay_success'  THEN 'success'
        WHEN 'vip_pay_fail'     THEN 'fail'
        WHEN 'vip_pay_cancel'   THEN 'cancel'
        ELSE 'unknown'
    END                                                     AS order_status,
    COALESCE(properties.plan_id, 'unknown')                 AS plan_id,
    COALESCE(properties.plan_name, 'unknown')               AS plan_name,
    -- plan_duration_days 只在 vip_select_plan 事件中有，订单事件填0，由离线层补全
    0                                                       AS plan_duration_days,
    COALESCE(properties.pay_method, 'unknown')              AS pay_method,
    COALESCE(app_context.platform, 'unknown')               AS platform,
    COALESCE(app_context.channel, 'unknown')                AS channel,
    -- from 字段只在 vip_page_view 事件中有，订单事件填 unknown
    'unknown'                                               AS from_page,
    COALESCE(properties.current_vip_type, 0)                AS current_vip_type,
    COALESCE(properties.coupon_id, '')                      AS coupon_id,
    COALESCE(properties.error_code, '')                     AS error_code,
    COALESCE(properties.cancel_stage, '')                   AS cancel_stage,

    -- ========== 用户维度（填占位符，由离线层补全）==========
    'unknown'                                               AS sex,
    -1                                                      AS age,
    -1                                                      AS birth_year,
    -1                                                      AS `level`,
    -1                                                      AS official_type,
    FALSE                                                   AS is_official,
    'unknown'                                               AS primary_tag,
    -1                                                      AS account_age_days,
    -1                                                      AS follower_cnt,

    -- ========== 聚合指标（当天累计值）==========
    COUNT(*)                                                AS order_cnt,
    COUNT(DISTINCT mid)                                     AS order_user_cnt,
    -- 金额单位为分，与batch层保持一致（不做分转元转换）
    CAST(SUM(COALESCE(properties.original_price, 0)) AS DECIMAL(12,2))
                                                            AS original_price,
    CAST(SUM(COALESCE(properties.final_price, 0)) AS DECIMAL(12,2))
                                                            AS final_price,
    CAST(SUM(COALESCE(properties.discount_amount, 0)) AS DECIMAL(12,2))
                                                            AS discount_amount,
    MAX(COALESCE(properties.pay_duration_sec, 0))           AS pay_duration_sec,

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP                                       AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd')                   AS dt

FROM TABLE(
    CUMULATE(
        TABLE kafka_event_vip,
        DESCRIPTOR(event_time),
        INTERVAL '15' SECOND,
        INTERVAL '1' DAY
    )
)
WHERE event_id IN ('vip_create_order', 'vip_pay_start', 'vip_pay_success', 'vip_pay_fail', 'vip_pay_cancel')
  AND mid IS NOT NULL
GROUP BY
    window_start,
    window_end,
    -- 订单维度分组
    CASE event_id
        WHEN 'vip_create_order' THEN 'created'
        WHEN 'vip_pay_start'    THEN 'paying'
        WHEN 'vip_pay_success'  THEN 'success'
        WHEN 'vip_pay_fail'     THEN 'fail'
        WHEN 'vip_pay_cancel'   THEN 'cancel'
        ELSE 'unknown'
    END,
    COALESCE(properties.plan_id, 'unknown'),
    COALESCE(properties.plan_name, 'unknown'),
    COALESCE(properties.pay_method, 'unknown'),
    COALESCE(app_context.platform, 'unknown'),
    COALESCE(app_context.channel, 'unknown'),
    COALESCE(properties.current_vip_type, 0),
    COALESCE(properties.coupon_id, ''),
    COALESCE(properties.error_code, ''),
    COALESCE(properties.cancel_stage, '');
