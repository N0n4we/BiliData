-- 每日聚合视频度量值
INSERT OVERWRITE TABLE dwd.dwd_stats_video_di
PARTITION (dt = '${bizdate}')
SELECT
    bvid,

    -- ========== 观看指标 ==========
    SUM(CASE WHEN event_id = 'video_view' THEN 1 ELSE 0 END)                AS view_count,
    COUNT(DISTINCT CASE WHEN event_id = 'video_view' THEN mid END)          AS view_user_count,
    SUM(CASE WHEN event_id = 'video_play_heartbeat' THEN 15 ELSE 0 END)     AS total_play_duration_sec,
    CAST(
        SUM(CASE WHEN event_id = 'video_play_heartbeat' THEN 15 ELSE 0 END) /
        NULLIF(COUNT(DISTINCT CASE WHEN event_id = 'video_view' THEN mid END), 0)
        AS INT
    )                                                                        AS avg_play_duration_sec,

    -- ========== 互动指标 ==========
    SUM(CASE WHEN event_id = 'video_like' THEN 1 ELSE 0 END)                AS like_count_delta,
    SUM(CASE WHEN event_id = 'video_unlike' THEN 1 ELSE 0 END)              AS unlike_count_delta,
    SUM(CASE WHEN event_id = 'video_like' THEN 1
             WHEN event_id = 'video_unlike' THEN -1
             ELSE 0 END)                                                     AS net_like_delta,

    SUM(CASE WHEN event_id = 'video_coin' THEN 1 ELSE 0 END)                AS coin_count_delta,
    SUM(CASE WHEN event_id = 'video_coin'
             THEN COALESCE(coin_count, 1) ELSE 0 END)                        AS coin_total_delta,

    SUM(CASE WHEN event_id = 'video_favorite' THEN 1 ELSE 0 END)            AS favorite_count_delta,
    SUM(CASE WHEN event_id = 'video_unfavorite' THEN 1 ELSE 0 END)          AS unfavorite_count_delta,
    SUM(CASE WHEN event_id = 'video_favorite' THEN 1
             WHEN event_id = 'video_unfavorite' THEN -1
             ELSE 0 END)                                                     AS net_favorite_delta,

    SUM(CASE WHEN event_id = 'video_share' THEN 1 ELSE 0 END)               AS share_count,
    SUM(CASE WHEN event_id = 'video_danmaku' THEN 1 ELSE 0 END)             AS danmaku_count,
    SUM(CASE WHEN event_id = 'video_triple' THEN 1 ELSE 0 END)              AS triple_count

FROM dwd.dwd_action_video_di
WHERE dt = '${bizdate}'
  AND bvid IS NOT NULL
GROUP BY bvid;
