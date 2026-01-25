-- 每日全量加载：统计数据完全从 dwd 累计，避免与 ODS 重复
WITH
-- ============================================================
-- 1. 昨日快照（获取累计统计值）
-- ============================================================
yesterday_snapshot AS (
    SELECT
        rpid,
        oid,
        like_count,
        dislike_count,
        reply_count
    FROM dim.dim_comment_df
    WHERE dt = DATE_SUB('${bizdate}', 1) AND rpid IS NOT NULL AND oid IS NOT NULL
),

-- ============================================================
-- 2. 今日 DWD 统计增量
-- ============================================================
today_stats AS (
    SELECT
        rpid,
        oid,
        net_like_delta,
        net_dislike_delta,
        reply_count_delta
    FROM dwd.dwd_stats_comment_di
    WHERE dt = '${bizdate}' AND rpid IS NOT NULL AND oid IS NOT NULL
),

-- ============================================================
-- 3. 合并 ODS 属性数据（不使用统计字段）
-- ============================================================
comment_attributes AS (
    SELECT
        rpid,
        oid,
        otype,
        mid,
        root,
        parent,
        content,
        state,
        is_root,
        created_at,
        updated_at,
        created_date,
        updated_date
    FROM (
        SELECT
            rpid,
            oid,
            otype,
            mid,
            root,
            parent,
            content,
            state,
            CASE WHEN root = 0 THEN TRUE ELSE FALSE END AS is_root,
            created_at,
            updated_at,
            FROM_UNIXTIME(created_at DIV 1000, 'yyyy-MM-dd') AS created_date,
            FROM_UNIXTIME(updated_at DIV 1000, 'yyyy-MM-dd') AS updated_date,
            ROW_NUMBER() OVER (
                PARTITION BY rpid, oid
                ORDER BY updated_at DESC
            ) AS rn
        FROM (
            -- 历史存量（前一天快照的属性）
            SELECT
                rpid, oid, otype, mid, root, parent, content, state,
                created_at, updated_at
            FROM dim.dim_comment_df
            WHERE dt = DATE_SUB('${bizdate}', 1)

            UNION ALL

            -- 当日增量（从 ODS 解析，不取统计字段）
            SELECT
                CAST(GET_JSON_OBJECT(json_str, '$.rpid') AS BIGINT)          AS rpid,
                GET_JSON_OBJECT(json_str, '$.oid')                           AS oid,
                CAST(GET_JSON_OBJECT(json_str, '$.otype') AS INT)            AS otype,
                CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)           AS mid,
                CAST(GET_JSON_OBJECT(json_str, '$.root') AS BIGINT)          AS root,
                CAST(GET_JSON_OBJECT(json_str, '$.parent') AS BIGINT)        AS parent,
                GET_JSON_OBJECT(json_str, '$.content')                       AS content,
                CAST(GET_JSON_OBJECT(json_str, '$.state') AS INT)            AS state,
                CAST(GET_JSON_OBJECT(json_str, '$.created_at') AS BIGINT)    AS created_at,
                CAST(GET_JSON_OBJECT(json_str, '$.updated_at') AS BIGINT)    AS updated_at
            FROM ods.ods_app_di
            WHERE dt = '${bizdate}'
              AND topic = 'app_comment'
        ) merged
    ) deduped
    WHERE rn = 1 AND rpid IS NOT NULL AND oid IS NOT NULL
),

-- ============================================================
-- 4. 获取所有评论 ID
-- ============================================================
all_comments AS (
    SELECT rpid, oid FROM comment_attributes
    UNION
    SELECT rpid, oid FROM today_stats
)

INSERT OVERWRITE TABLE dim.dim_comment_df PARTITION (dt = '${bizdate}')
SELECT
    c.rpid,
    c.oid,
    a.otype,
    a.mid,
    a.root,
    a.parent,
    a.content,
    -- ========== 统计数据：完全从 DWD 累计 ==========
    COALESCE(y.like_count, 0) + COALESCE(s.net_like_delta, 0)       AS like_count,
    COALESCE(y.dislike_count, 0) + COALESCE(s.net_dislike_delta, 0) AS dislike_count,
    COALESCE(y.reply_count, 0) + COALESCE(s.reply_count_delta, 0)   AS reply_count,
    a.state,
    a.is_root,
    a.created_at,
    CASE
        WHEN s.rpid IS NOT NULL
        THEN CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)
        ELSE a.updated_at
    END                                                              AS updated_at,
    a.created_date,
    CASE
        WHEN s.rpid IS NOT NULL
        THEN '${bizdate}'
        ELSE a.updated_date
    END                                                              AS updated_date

FROM all_comments c
LEFT JOIN comment_attributes a ON c.rpid = a.rpid AND c.oid = a.oid
LEFT JOIN yesterday_snapshot y ON c.rpid = y.rpid AND c.oid = y.oid
LEFT JOIN today_stats s ON c.rpid = s.rpid AND c.oid = s.oid;
