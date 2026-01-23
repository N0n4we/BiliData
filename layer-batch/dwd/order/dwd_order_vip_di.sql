-- =====================================================
-- dwd_order_vip_di T+1 加工脚本
-- 从ODS取当日每个订单的最新状态
-- =====================================================

INSERT OVERWRITE TABLE dwd.dwd_order_vip_di PARTITION (dt = '${bizdate}')
SELECT
    order_no,
    mid,
    current_vip_type,
    current_vip_expire_ts,
    plan_id,
    plan_name,
    plan_duration_days,
    original_price,
    final_price,
    discount_amount,
    coupon_id,
    pay_method,
    pay_duration_sec,
    new_vip_expire_ts,
    order_status,
    create_ts,
    pay_start_ts,
    pay_end_ts,
    error_code,
    error_msg,
    retry_count,
    cancel_stage,
    device_id,
    platform,
    app_version,
    channel,
    ip,
    from_page,
    trace_id,
    session_id
FROM (
    SELECT
        order_no,

        -- 取各字段最新非空值
        MAX(mid)                     AS mid,
        MAX(current_vip_type)        AS current_vip_type,
        MAX(current_vip_expire_ts)   AS current_vip_expire_ts,
        MAX(plan_id)                 AS plan_id,
        MAX(plan_name)               AS plan_name,
        MAX(plan_duration_days)      AS plan_duration_days,
        MAX(original_price)          AS original_price,
        MAX(final_price)             AS final_price,
        MAX(discount_amount)         AS discount_amount,
        MAX(coupon_id)               AS coupon_id,
        MAX(pay_method)              AS pay_method,
        MAX(pay_duration_sec)        AS pay_duration_sec,
        MAX(new_vip_expire_ts)       AS new_vip_expire_ts,

        -- 状态推导: success > fail > cancel > paying > created
        CASE
            WHEN SUM(CASE WHEN event_id = 'vip_pay_success' THEN 1 ELSE 0 END) > 0 THEN 'success'
            WHEN SUM(CASE WHEN event_id = 'vip_pay_fail' THEN 1 ELSE 0 END) > 0    THEN 'fail'
            WHEN SUM(CASE WHEN event_id = 'vip_pay_cancel' THEN 1 ELSE 0 END) > 0  THEN 'cancel'
            WHEN SUM(CASE WHEN event_id = 'vip_pay_start' THEN 1 ELSE 0 END) > 0   THEN 'paying'
            ELSE 'created'
        END AS order_status,

        -- 里程碑时间
        MIN(CASE WHEN event_id = 'vip_create_order' THEN server_ts END) AS create_ts,
        MIN(CASE WHEN event_id = 'vip_pay_start' THEN server_ts END)    AS pay_start_ts,
        MAX(CASE WHEN event_id IN ('vip_pay_success','vip_pay_fail','vip_pay_cancel')
                 THEN server_ts END) AS pay_end_ts,

        -- 异常信息
        MAX(error_code)              AS error_code,
        MAX(error_msg)               AS error_msg,
        MAX(retry_count)             AS retry_count,
        MAX(cancel_stage)            AS cancel_stage,

        -- 设备环境(取最早的)
        MIN(device_id)               AS device_id,
        MIN(platform)                AS platform,
        MIN(app_version)             AS app_version,
        MIN(channel)                 AS channel,
        MIN(ip)                      AS ip,
        MIN(from_page)               AS from_page,
        MIN(trace_id)                AS trace_id,
        MIN(session_id)              AS session_id

    FROM (
        -- 解析ODS原始JSON
        SELECT
            GET_JSON_OBJECT(json_str, '$.event_id')                                      AS event_id,
            GET_JSON_OBJECT(json_str, '$.order_no')                                      AS order_no,
            CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                           AS mid,
            CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT)                     AS server_ts,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.current_vip_type') AS INT)      AS current_vip_type,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.current_vip_expire') AS BIGINT) AS current_vip_expire_ts,
            GET_JSON_OBJECT(json_str, '$.properties.plan_id')                            AS plan_id,
            GET_JSON_OBJECT(json_str, '$.properties.plan_name')                          AS plan_name,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.plan_duration_days') AS INT)    AS plan_duration_days,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.original_price') AS BIGINT)     AS original_price,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.final_price') AS BIGINT)        AS final_price,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.discount_amount') AS BIGINT)    AS discount_amount,
            GET_JSON_OBJECT(json_str, '$.properties.coupon_id')                          AS coupon_id,
            GET_JSON_OBJECT(json_str, '$.properties.pay_method')                         AS pay_method,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.pay_duration_sec') AS INT)      AS pay_duration_sec,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.new_vip_expire') AS BIGINT)     AS new_vip_expire_ts,
            GET_JSON_OBJECT(json_str, '$.properties.error_code')                         AS error_code,
            GET_JSON_OBJECT(json_str, '$.properties.error_msg')                          AS error_msg,
            CAST(GET_JSON_OBJECT(json_str, '$.properties.retry_count') AS INT)           AS retry_count,
            GET_JSON_OBJECT(json_str, '$.properties.cancel_stage')                       AS cancel_stage,
            GET_JSON_OBJECT(json_str, '$.device_info.device_id')                         AS device_id,
            GET_JSON_OBJECT(json_str, '$.app_context.platform')                          AS platform,
            GET_JSON_OBJECT(json_str, '$.app_context.app_version')                       AS app_version,
            GET_JSON_OBJECT(json_str, '$.app_context.channel')                           AS channel,
            GET_JSON_OBJECT(json_str, '$.ip')                                            AS ip,
            GET_JSON_OBJECT(json_str, '$.properties.from')                               AS from_page,
            GET_JSON_OBJECT(json_str, '$.trace_id')                                      AS trace_id,
            GET_JSON_OBJECT(json_str, '$.session_id')                                    AS session_id
        FROM ods.ods_app_di
        WHERE dt = '${bizdate}'
          AND topic = 'app_event_vip'
          AND GET_JSON_OBJECT(json_str, '$.order_no') IS NOT NULL
          AND GET_JSON_OBJECT(json_str, '$.order_no') != ''
    ) parsed
    GROUP BY order_no
) agg
WHERE order_no IS NOT NULL;
