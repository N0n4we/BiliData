WITH
-- ============================================================
-- 1. 昨日快照（获取累计统计值）
-- ============================================================
yesterday_snapshot AS (
    SELECT
        bvid,
        view_count,
        danmaku_count,
        reply_count,
        favorite_count,
        coin_count,
        share_count,
        like_count
    FROM dim.dim_video_df
    WHERE dt = DATE_SUB('${bizdate}', 1)
),

-- ============================================================
-- 2. 今日 DWD 统计增量
-- ============================================================
today_stats AS (
    SELECT
        bvid,
        view_count              AS view_delta,
        danmaku_count           AS danmaku_delta,
        net_favorite_delta      AS favorite_delta,
        coin_total_delta        AS coin_delta,
        share_count             AS share_delta,
        net_like_delta          AS like_delta
    FROM dwd.dwd_stats_video_di
    WHERE dt = '${bizdate}'
),

-- ============================================================
-- 3. 今日评论统计（reply_count 从评论行为统计）
-- ============================================================
today_reply_stats AS (
    SELECT
        oid                     AS bvid,
        COUNT(*)                AS reply_delta
    FROM dwd.dwd_action_comment_di
    WHERE dt = '${bizdate}'
      AND event_id = 'comment_create'
      AND oid IS NOT NULL
    GROUP BY oid
),

-- ============================================================
-- 4. 合并 ODS 属性数据（不使用 stats 字段的展开值）
-- ============================================================
video_attributes AS (
    SELECT
        bvid,
        title,
        cover,
        desc_text,
        duration,
        pubdate,
        mid,
        category_id,
        category_name,
        state,
        attribute,
        is_private,
        upload_ip,
        camera,
        software,
        resolution,
        fps,
        created_at,
        updated_at,
        created_date,
        updated_date,
        pub_date,
        audit_info,
        meta_info,
        stats  -- 保留原始 JSON，但不展开使用
    FROM (
        SELECT
            bvid,
            title,
            cover,
            desc_text,
            duration,
            pubdate,
            mid,
            category_id,
            category_name,
            state,
            attribute,
            is_private,
            upload_ip,
            camera,
            software,
            resolution,
            fps,
            created_at,
            updated_at,
            created_date,
            updated_date,
            pub_date,
            audit_info,
            meta_info,
            stats,
            ROW_NUMBER() OVER (
                PARTITION BY bvid
                ORDER BY updated_at DESC
            ) AS rn
        FROM (
            -- 历史存量（前一天快照的属性）
            SELECT
                bvid, title, cover, desc_text, duration, pubdate, mid,
                category_id, category_name, state, attribute, is_private,
                upload_ip, camera, software, resolution, fps,
                created_at, updated_at, created_date, updated_date, pub_date,
                audit_info, meta_info, stats
            FROM dim.dim_video_df
            WHERE dt = DATE_SUB('${bizdate}', 1)

            UNION ALL

            -- 当日增量（从 ODS 解析，只取属性，不展开 stats）
            SELECT
                GET_JSON_OBJECT(json_str, '$.bvid')                                      AS bvid,
                GET_JSON_OBJECT(json_str, '$.title')                                     AS title,
                GET_JSON_OBJECT(json_str, '$.cover')                                     AS cover,
                GET_JSON_OBJECT(json_str, '$.desc_text')                                 AS desc_text,
                CAST(GET_JSON_OBJECT(json_str, '$.duration') AS INT)                     AS duration,
                CAST(GET_JSON_OBJECT(json_str, '$.pubdate') AS BIGINT)                   AS pubdate,
                CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                       AS mid,
                CAST(GET_JSON_OBJECT(json_str, '$.category_id') AS INT)                  AS category_id,
                GET_JSON_OBJECT(json_str, '$.category_name')                             AS category_name,
                CAST(GET_JSON_OBJECT(json_str, '$.state') AS INT)                        AS state,
                CAST(GET_JSON_OBJECT(json_str, '$.attribute') AS INT)                    AS attribute,
                CAST(GET_JSON_OBJECT(json_str, '$.is_private') AS BOOLEAN)               AS is_private,
                GET_JSON_OBJECT(json_str, '$.meta_info.upload_ip')                       AS upload_ip,
                GET_JSON_OBJECT(json_str, '$.meta_info.camera')                          AS camera,
                GET_JSON_OBJECT(json_str, '$.meta_info.software')                        AS software,
                GET_JSON_OBJECT(json_str, '$.meta_info.resolution')                      AS resolution,
                CAST(GET_JSON_OBJECT(json_str, '$.meta_info.fps') AS INT)                AS fps,
                CAST(GET_JSON_OBJECT(json_str, '$.created_at') AS BIGINT)                AS created_at,
                CAST(GET_JSON_OBJECT(json_str, '$.updated_at') AS BIGINT)                AS updated_at,
                FROM_UNIXTIME(
                    CAST(GET_JSON_OBJECT(json_str, '$.created_at') AS BIGINT) DIV 1000,
                    'yyyy-MM-dd'
                )                                                                         AS created_date,
                FROM_UNIXTIME(
                    CAST(GET_JSON_OBJECT(json_str, '$.updated_at') AS BIGINT) DIV 1000,
                    'yyyy-MM-dd'
                )                                                                         AS updated_date,
                FROM_UNIXTIME(
                    CAST(GET_JSON_OBJECT(json_str, '$.pubdate') AS BIGINT) DIV 1000,
                    'yyyy-MM-dd'
                )                                                                         AS pub_date,
                GET_JSON_OBJECT(json_str, '$.audit_info')                                AS audit_info,
                GET_JSON_OBJECT(json_str, '$.meta_info')                                 AS meta_info,
                GET_JSON_OBJECT(json_str, '$.stats')                                     AS stats
            FROM ods.ods_app_di
            WHERE dt = '${bizdate}'
              AND topic = 'app_video_content'
        ) merged
    ) deduped
    WHERE rn = 1
),

-- ============================================================
-- 5. 获取所有视频 ID（用于确保新老视频都被处理）
-- ============================================================
all_videos AS (
    SELECT bvid FROM video_attributes
    UNION
    SELECT bvid FROM today_stats
)

INSERT OVERWRITE TABLE dim.dim_video_df PARTITION (dt = '${bizdate}')
SELECT
    v.bvid,
    a.title,
    a.cover,
    a.desc_text,
    a.duration,
    a.pubdate,
    a.mid,
    a.category_id,
    a.category_name,
    a.state,
    a.attribute,
    a.is_private,

    -- ========== 元信息 ==========
    a.upload_ip,
    a.camera,
    a.software,
    a.resolution,
    a.fps,

    -- ========== 统计数据：完全从 DWD 累计，不使用 ODS stats ==========
    COALESCE(y.view_count, 0) + COALESCE(s.view_delta, 0)           AS view_count,
    COALESCE(y.danmaku_count, 0) + COALESCE(s.danmaku_delta, 0)     AS danmaku_count,
    COALESCE(y.reply_count, 0) + COALESCE(r.reply_delta, 0)         AS reply_count,
    COALESCE(y.favorite_count, 0) + COALESCE(s.favorite_delta, 0)   AS favorite_count,
    COALESCE(y.coin_count, 0) + COALESCE(s.coin_delta, 0)           AS coin_count,
    COALESCE(y.share_count, 0) + COALESCE(s.share_delta, 0)         AS share_count,
    COALESCE(y.like_count, 0) + COALESCE(s.like_delta, 0)           AS like_count,

    -- ========== 时间信息 ==========
    a.created_at,
    CASE
        WHEN s.bvid IS NOT NULL OR r.bvid IS NOT NULL
        THEN CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)
        ELSE a.updated_at
    END                                                              AS updated_at,
    a.created_date,
    CASE
        WHEN s.bvid IS NOT NULL OR r.bvid IS NOT NULL
        THEN '${bizdate}'
        ELSE a.updated_date
    END                                                              AS updated_date,
    a.pub_date,

    -- ========== 原始数据（保留但不使用） ==========
    a.audit_info,
    a.meta_info,
    a.stats

FROM all_videos v
LEFT JOIN video_attributes a ON v.bvid = a.bvid
LEFT JOIN yesterday_snapshot y ON v.bvid = y.bvid
LEFT JOIN today_stats s ON v.bvid = s.bvid
LEFT JOIN today_reply_stats r ON v.bvid = r.bvid
WHERE v.bvid IS NOT NULL;
