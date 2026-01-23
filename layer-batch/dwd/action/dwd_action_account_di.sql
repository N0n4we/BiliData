INSERT OVERWRITE TABLE dwd.dwd_action_account_di PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 事件标识 ==========
    GET_JSON_OBJECT(json_str, '$.event_id')                                         AS event_id,
    GET_JSON_OBJECT(json_str, '$.trace_id')                                         AS trace_id,
    GET_JSON_OBJECT(json_str, '$.session_id')                                       AS session_id,

    -- ========== 用户信息 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                              AS mid,
    CAST(GET_JSON_OBJECT(json_str, '$.target_mid') AS BIGINT)                       AS target_mid,

    -- ========== 时间信息 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.client_ts') AS BIGINT)                        AS client_ts,
    CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT)                        AS server_ts,
    CASE
        WHEN GET_JSON_OBJECT(json_str, '$.server_ts') IS NOT NULL
             AND GET_JSON_OBJECT(json_str, '$.server_ts') RLIKE '^[0-9]+$'
        THEN FROM_UNIXTIME(CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT) DIV 1000)
        ELSE NULL
    END                                                                              AS event_time,

    -- ========== 页面信息 ==========
    GET_JSON_OBJECT(json_str, '$.url_path')                                         AS url_path,
    GET_JSON_OBJECT(json_str, '$.referer')                                          AS referer,

    -- ========== 设备信息 ==========
    GET_JSON_OBJECT(json_str, '$.device_info.device_id')                            AS device_id,
    GET_JSON_OBJECT(json_str, '$.device_info.brand')                                AS brand,
    GET_JSON_OBJECT(json_str, '$.device_info.model')                                AS model,
    GET_JSON_OBJECT(json_str, '$.device_info.os')                                   AS os,
    GET_JSON_OBJECT(json_str, '$.device_info.os_version')                           AS os_version,
    GET_JSON_OBJECT(json_str, '$.device_info.network')                              AS network,
    GET_JSON_OBJECT(json_str, '$.ip')                                               AS ip,

    -- ========== APP上下文 ==========
    GET_JSON_OBJECT(json_str, '$.app_context.app_version')                          AS app_version,
    GET_JSON_OBJECT(json_str, '$.app_context.channel')                              AS channel,
    GET_JSON_OBJECT(json_str, '$.app_context.platform')                             AS platform,

    -- ========== 通用行为属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.from')                                  AS action_from,

    -- ========== 关注行为属性 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.properties.target_level') AS INT)             AS target_level,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.target_follower_count') AS BIGINT) AS target_follower_cnt,
    CASE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_mutual')) = 'true' THEN TRUE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_mutual')) = 'false' THEN FALSE
        ELSE NULL
    END                                                                              AS is_mutual,
    GET_JSON_OBJECT(json_str, '$.properties.special_group')                         AS special_group,

    -- ========== 取关行为属性 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.properties.follow_duration_days') AS INT)     AS follow_duration_days,
    GET_JSON_OBJECT(json_str, '$.properties.unfollow_reason')                       AS unfollow_reason,

    -- ========== 拉黑行为属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.block_reason')                          AS block_reason,
    CASE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_following')) = 'true' THEN TRUE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_following')) = 'false' THEN FALSE
        ELSE NULL
    END                                                                              AS is_following,

    -- ========== 私信行为属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.msg_type')                              AS msg_type,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.msg_length') AS INT)               AS msg_length,
    CASE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_first_msg')) = 'true' THEN TRUE
        WHEN LOWER(GET_JSON_OBJECT(json_str, '$.properties.is_first_msg')) = 'false' THEN FALSE
        ELSE NULL
    END                                                                              AS is_first_msg,

    -- ========== 举报行为属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.report_reason')                         AS report_reason,
    GET_JSON_OBJECT(json_str, '$.properties.report_evidence')                       AS report_evidence

FROM ods.ods_app_di
WHERE dt = '${bizdate}'
  AND topic = 'app_event_social'
  AND GET_JSON_OBJECT(json_str, '$.event_id') IS NOT NULL
  AND GET_JSON_OBJECT(json_str, '$.mid') IS NOT NULL;
