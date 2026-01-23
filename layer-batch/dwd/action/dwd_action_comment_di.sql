-- 每日增量加载评论行为事件
INSERT OVERWRITE TABLE dwd.dwd_action_comment_di
PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 事件标识 ==========
    GET_JSON_OBJECT(json_str, '$.event_id')                                      AS event_id,
    GET_JSON_OBJECT(json_str, '$.trace_id')                                      AS trace_id,
    GET_JSON_OBJECT(json_str, '$.session_id')                                    AS session_id,

    -- ========== 用户与对象 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                           AS mid,
    GET_JSON_OBJECT(json_str, '$.oid')                                           AS oid,
    CAST(GET_JSON_OBJECT(json_str, '$.rpid') AS BIGINT)                          AS rpid,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.otype') AS INT)                 AS otype,

    -- ========== 时间信息 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.client_ts') AS BIGINT)                     AS client_ts,
    CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT)                     AS server_ts,
    FROM_UNIXTIME(
        CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT) DIV 1000,
        'yyyy-MM-dd HH:mm:ss'
    )                                                                            AS event_time,
    FROM_UNIXTIME(
        CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT) DIV 1000,
        'yyyy-MM-dd'
    )                                                                            AS event_date,
    FROM_UNIXTIME(
        CAST(GET_JSON_OBJECT(json_str, '$.server_ts') AS BIGINT) DIV 1000,
        'HH'
    )                                                                            AS event_hour,

    -- ========== 页面信息 ==========
    GET_JSON_OBJECT(json_str, '$.url_path')                                      AS url_path,
    GET_JSON_OBJECT(json_str, '$.referer')                                       AS referer,
    GET_JSON_OBJECT(json_str, '$.ua')                                            AS ua,
    GET_JSON_OBJECT(json_str, '$.ip')                                            AS ip,

    -- ========== 设备信息（展开） ==========
    GET_JSON_OBJECT(json_str, '$.device_info.device_id')                         AS device_id,
    GET_JSON_OBJECT(json_str, '$.device_info.brand')                             AS brand,
    GET_JSON_OBJECT(json_str, '$.device_info.model')                             AS model,
    GET_JSON_OBJECT(json_str, '$.device_info.os')                                AS os,
    GET_JSON_OBJECT(json_str, '$.device_info.os_version')                        AS os_version,
    CAST(GET_JSON_OBJECT(json_str, '$.device_info.screen_width') AS INT)         AS screen_width,
    CAST(GET_JSON_OBJECT(json_str, '$.device_info.screen_height') AS INT)        AS screen_height,
    GET_JSON_OBJECT(json_str, '$.device_info.carrier')                           AS carrier,
    GET_JSON_OBJECT(json_str, '$.device_info.network')                           AS network,

    -- ========== APP上下文（展开） ==========
    GET_JSON_OBJECT(json_str, '$.app_context.app_version')                       AS app_version,
    GET_JSON_OBJECT(json_str, '$.app_context.platform')                          AS platform,
    GET_JSON_OBJECT(json_str, '$.app_context.channel')                           AS channel,

    -- ========== 评论创建属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.content')                            AS content,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.content_length') AS INT)        AS content_length,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.has_emoji') AS BOOLEAN)         AS has_emoji,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.has_at') AS BOOLEAN)            AS has_at,
    GET_JSON_OBJECT(json_str, '$.properties.from')                               AS from_page,

    -- ========== 评论回复属性 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.properties.root_rpid') AS BIGINT)          AS root_rpid,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.parent_rpid') AS BIGINT)        AS parent_rpid,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.reply_to_mid') AS BIGINT)       AS reply_to_mid,

    -- ========== 评论互动属性 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.properties.is_root_comment') AS BOOLEAN)   AS is_root_comment,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.comment_owner_mid') AS BIGINT)  AS comment_owner_mid,

    -- ========== 评论修改/删除属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.old_content')                        AS old_content,
    GET_JSON_OBJECT(json_str, '$.properties.new_content')                        AS new_content,
    GET_JSON_OBJECT(json_str, '$.properties.edit_reason')                        AS edit_reason,
    GET_JSON_OBJECT(json_str, '$.properties.delete_reason')                      AS delete_reason,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.comment_age_hours') AS INT)     AS comment_age_hours,

    -- ========== 举报属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.report_reason')                      AS report_reason,
    GET_JSON_OBJECT(json_str, '$.properties.report_content_preview')             AS report_content_preview,

    -- ========== 原始数据 ==========
    GET_JSON_OBJECT(json_str, '$.properties')                                    AS properties,
    GET_JSON_OBJECT(json_str, '$.device_info')                                   AS device_info,
    GET_JSON_OBJECT(json_str, '$.app_context')                                   AS app_context

FROM ods.ods_app_di
WHERE dt = '${bizdate}'
  AND topic = 'app_event_comment';
