-- UP主视频统计明细表（视频粒度，每日增量）
-- 以bvid为粒度，关联视频维度和账号维度信息
INSERT OVERWRITE TABLE dws.dws_video_stats_account_di_v2
PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 视频维度信息 ==========
    s.bvid,
    v.title,
    v.duration,
    v.pubdate,
    v.pub_date,
    v.category_id,
    v.category_name,

    -- ========== 账号维度信息 ==========
    v.mid,
    a.nick_name,
    a.sex,
    a.level,
    a.vip_type,
    a.vip_type_name,
    a.official_type,
    a.official_desc,
    a.is_official,
    a.follower_cnt,
    a.following_cnt,

    -- ========== 视频统计指标 ==========
    -- 观看指标
    s.view_count,
    s.view_user_count,
    s.total_play_duration_sec,
    s.avg_play_duration_sec,

    -- 互动指标
    s.like_count_delta          AS like_delta,
    s.unlike_count_delta        AS unlike_delta,
    s.net_like_delta,

    s.coin_count_delta,
    s.coin_total_delta,

    s.favorite_count_delta      AS favorite_delta,
    s.unfavorite_count_delta    AS unfavorite_delta,
    s.net_favorite_delta,

    s.share_count,
    s.danmaku_count,
    s.triple_count,

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP AS dw_create_time

FROM dwd.dwd_stats_video_di s
INNER JOIN dim.dim_video_df v
    ON s.bvid = v.bvid
    AND v.dt = '${bizdate}'
LEFT JOIN dim.dim_account_df a
    ON v.mid = a.mid
    AND a.dt = '${bizdate}'
WHERE s.dt = '${bizdate}';
