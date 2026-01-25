-- UP主视频统计汇总表（每日增量）
-- 以mid为粒度聚合视频统计数据，并关联账号维度信息
INSERT OVERWRITE TABLE dws.dws_video_stats_account_di
PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 账号维度信息 ==========
    a.mid,
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

    -- ========== 视频统计汇总 ==========
    agg.video_cnt,                          -- 当日有数据的视频数

    -- 观看指标汇总
    agg.total_view_count,                   -- 总观看次数
    agg.total_view_user_count,              -- 总观看用户数
    agg.total_play_duration_sec,            -- 总播放时长(秒)
    agg.avg_play_duration_sec,              -- 平均播放时长(秒)

    -- 互动指标汇总
    agg.total_like_delta,                   -- 总点赞增量
    agg.total_unlike_delta,                 -- 总取消点赞增量
    agg.total_net_like_delta,               -- 总净点赞增量

    agg.total_coin_count_delta,             -- 总投币次数增量
    agg.total_coin_total_delta,             -- 总投币数增量

    agg.total_favorite_delta,               -- 总收藏增量
    agg.total_unfavorite_delta,             -- 总取消收藏增量
    agg.total_net_favorite_delta,           -- 总净收藏增量

    agg.total_share_count,                  -- 总分享次数
    agg.total_danmaku_count,                -- 总弹幕数
    agg.total_triple_count,                 -- 总一键三连次数

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP AS dw_create_time

FROM (
    -- 以mid为粒度聚合视频统计数据
    SELECT
        v.mid,
        COUNT(DISTINCT s.bvid)              AS video_cnt,

        -- 观看指标
        SUM(s.view_count)                   AS total_view_count,
        SUM(s.view_user_count)              AS total_view_user_count,
        SUM(s.total_play_duration_sec)      AS total_play_duration_sec,
        CAST(
            SUM(s.total_play_duration_sec) /
            NULLIF(SUM(s.view_user_count), 0)
            AS INT
        )                                   AS avg_play_duration_sec,

        -- 互动指标
        SUM(s.like_count_delta)             AS total_like_delta,
        SUM(s.unlike_count_delta)           AS total_unlike_delta,
        SUM(s.net_like_delta)               AS total_net_like_delta,

        SUM(s.coin_count_delta)             AS total_coin_count_delta,
        SUM(s.coin_total_delta)             AS total_coin_total_delta,

        SUM(s.favorite_count_delta)         AS total_favorite_delta,
        SUM(s.unfavorite_count_delta)       AS total_unfavorite_delta,
        SUM(s.net_favorite_delta)           AS total_net_favorite_delta,

        SUM(s.share_count)                  AS total_share_count,
        SUM(s.danmaku_count)                AS total_danmaku_count,
        SUM(s.triple_count)                 AS total_triple_count

    FROM dwd.dwd_stats_video_di s
    -- TODO: 重构dwd_stats_video_di使其包含mid列表，避免这次连接
    INNER JOIN dim.dim_video_df v
        ON s.bvid = v.bvid
        AND v.dt = '${bizdate}'
    WHERE s.dt = '${bizdate}'
    GROUP BY v.mid
) agg
LEFT JOIN dim.dim_account_df a
    ON agg.mid = a.mid
    AND a.dt = '${bizdate}';
