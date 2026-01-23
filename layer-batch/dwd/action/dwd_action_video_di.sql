-- 每日增量加载视频行为事件
INSERT OVERWRITE TABLE dwd.dwd_action_video_di
PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 数据质量校验 ==========
    CASE
        WHEN GET_JSON_OBJECT(json_str, '$.event_id') IS NULL THEN FALSE
        WHEN GET_JSON_OBJECT(json_str, '$.mid') IS NULL THEN FALSE
        WHEN GET_JSON_OBJECT(json_str, '$.server_ts') IS NULL THEN FALSE
        ELSE TRUE
    END AS is_valid,
    CASE
        WHEN GET_JSON_OBJECT(json_str, '$.event_id') IS NULL THEN 'missing_event_id'
        WHEN GET_JSON_OBJECT(json_str, '$.mid') IS NULL THEN 'missing_mid'
        WHEN GET_JSON_OBJECT(json_str, '$.server_ts') IS NULL THEN 'missing_server_ts'
        ELSE NULL
    END AS invalid_reason,

    -- ========== 事件标识 ==========
    GET_JSON_OBJECT(json_str, '$.event_id')                                      AS event_id,
    GET_JSON_OBJECT(json_str, '$.trace_id')                                      AS trace_id,
    GET_JSON_OBJECT(json_str, '$.session_id')                                    AS session_id,

    -- ========== 用户与对象 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                           AS mid,
    GET_JSON_OBJECT(json_str, '$.bvid')                                          AS bvid,

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

    -- ========== 观看属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.from')                               AS view_from,
    GET_JSON_OBJECT(json_str, '$.properties.spm_id')                             AS spm_id,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.is_auto_play') AS BOOLEAN)      AS is_auto_play,
    GET_JSON_OBJECT(json_str, '$.properties.quality')                            AS quality,

    -- ========== 播放心跳属性 ==========
    CAST(GET_JSON_OBJECT(json_str, '$.properties.progress_sec') AS INT)          AS progress_sec,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.total_sec') AS INT)             AS total_sec,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.play_speed') AS DOUBLE)         AS play_speed,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.is_full_screen') AS BOOLEAN)    AS is_full_screen,

    -- ========== 上传属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.upload_id')                          AS upload_id,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.file_size_mb') AS INT)          AS file_size_mb,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.duration_sec') AS INT)          AS duration_sec,
    GET_JSON_OBJECT(json_str, '$.properties.resolution')                         AS resolution,
    GET_JSON_OBJECT(json_str, '$.properties.upload_type')                        AS upload_type,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.progress_percent') AS INT)      AS progress_percent,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.uploaded_mb') AS INT)           AS uploaded_mb,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.speed_kbps') AS INT)            AS speed_kbps,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.upload_duration_sec') AS INT)   AS upload_duration_sec,
    GET_JSON_OBJECT(json_str, '$.properties.title')                              AS title,
    GET_JSON_OBJECT(json_str, '$.properties.category')                           AS category,

    -- ========== 上传失败属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.error_code')                         AS error_code,
    GET_JSON_OBJECT(json_str, '$.properties.error_msg')                          AS error_msg,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.retry_count') AS INT)           AS retry_count,

    -- ========== 修改/删除属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.updated_fields')                     AS updated_fields,
    GET_JSON_OBJECT(json_str, '$.properties.delete_reason')                      AS delete_reason,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.video_age_days') AS INT)        AS video_age_days,

    -- ========== 互动属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.from')                               AS action_from,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.is_liked_before') AS BOOLEAN)   AS is_liked_before,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.coin_count') AS INT)            AS coin_count,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.with_like') AS BOOLEAN)         AS with_like,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.remain_coins') AS INT)          AS remain_coins,
    GET_JSON_OBJECT(json_str, '$.properties.fav_folder_ids')                     AS fav_folder_ids,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.create_new_folder') AS BOOLEAN) AS create_new_folder,
    GET_JSON_OBJECT(json_str, '$.properties.share_channel')                      AS share_channel,
    GET_JSON_OBJECT(json_str, '$.properties.share_result')                       AS share_result,

    -- ========== 弹幕属性 ==========
    GET_JSON_OBJECT(json_str, '$.properties.content')                            AS danmaku_content,
    CAST(GET_JSON_OBJECT(json_str, '$.properties.progress_sec') AS INT)          AS danmaku_progress_sec,
    GET_JSON_OBJECT(json_str, '$.properties.danmaku_type')                       AS danmaku_type,
    GET_JSON_OBJECT(json_str, '$.properties.color')                              AS danmaku_color,

    -- ========== 原始数据 ==========
    GET_JSON_OBJECT(json_str, '$.properties')                                    AS properties,
    GET_JSON_OBJECT(json_str, '$.device_info')                                   AS device_info,
    GET_JSON_OBJECT(json_str, '$.app_context')                                   AS app_context

FROM ods.ods_app_di
WHERE dt = '${bizdate}'
  AND topic = 'app_event_video';
