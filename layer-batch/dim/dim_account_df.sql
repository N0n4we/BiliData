WITH
-- ============================================================
-- 1. 用户基础资料
-- ============================================================
user_profile_parsed AS (
    SELECT
        CAST(GET_JSON_OBJECT(json_str, '$.mid') AS BIGINT)                              AS mid,
        GET_JSON_OBJECT(json_str, '$.nick_name')                                        AS nick_name,
        GET_JSON_OBJECT(json_str, '$.sex')                                              AS sex,
        GET_JSON_OBJECT(json_str, '$.face_url')                                         AS face_url,
        GET_JSON_OBJECT(json_str, '$.sign')                                             AS sign,
        CAST(GET_JSON_OBJECT(json_str, '$.level') AS INT)                               AS level,
        GET_JSON_OBJECT(json_str, '$.birthday')                                         AS birthday,
        CAST(GET_JSON_OBJECT(json_str, '$.coins') AS DECIMAL(12,2))                     AS coins,
        CAST(GET_JSON_OBJECT(json_str, '$.status') AS INT)                              AS status,
        CAST(GET_JSON_OBJECT(json_str, '$.official_verify.type') AS INT)                AS official_type,
        GET_JSON_OBJECT(json_str, '$.official_verify.desc')                             AS official_desc,
        CAST(GET_JSON_OBJECT(json_str, '$.settings.privacy.show_fav') AS BOOLEAN)       AS privacy_show_fav,
        CAST(GET_JSON_OBJECT(json_str, '$.settings.privacy.show_history') AS BOOLEAN)   AS privacy_show_history,
        CAST(GET_JSON_OBJECT(json_str, '$.settings.push.comment') AS BOOLEAN)           AS push_comment,
        CAST(GET_JSON_OBJECT(json_str, '$.settings.push.like') AS BOOLEAN)              AS push_like,
        CAST(GET_JSON_OBJECT(json_str, '$.settings.push.at') AS BOOLEAN)                AS push_at,
        GET_JSON_OBJECT(json_str, '$.settings.theme')                                   AS theme,
        GET_JSON_OBJECT(json_str, '$.tags')                                             AS tags_json,
        CAST(GET_JSON_OBJECT(json_str, '$.created_at') AS BIGINT)                       AS created_at_ts,
        CAST(GET_JSON_OBJECT(json_str, '$.updated_at') AS BIGINT)                       AS updated_at_ts,
        ROW_NUMBER() OVER (
            PARTITION BY GET_JSON_OBJECT(json_str, '$.mid')
            ORDER BY CAST(GET_JSON_OBJECT(json_str, '$.updated_at') AS BIGINT) DESC
        ) AS rn
    FROM ods.ods_app_di
    WHERE dt = '${bizdate}'
      AND topic = 'app_user_profile'
      AND GET_JSON_OBJECT(json_str, '$.mid') IS NOT NULL
),

user_profile_latest AS (
    SELECT
        p.*,
        CASE
            WHEN p.tags_json IS NULL OR p.tags_json = '' OR p.tags_json = '[]'
            THEN ARRAY()
            ELSE SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(p.tags_json, '\\[|\\]', ''), '"', ''), ',')
        END AS tags_array
    FROM user_profile_parsed p
    WHERE rn = 1
),

-- ============================================================
-- 2. 昨日累计状态（关系 + VIP信息）
-- ============================================================
yesterday_snapshot AS (
    SELECT
        mid,
        -- 关系数据
        following_list      AS yd_following_list,
        follower_list       AS yd_follower_list,
        blocking_list       AS yd_blocking_list,
        blocked_by_list     AS yd_blocked_by_list,
        -- VIP数据
        vip_type            AS yd_vip_type,
        vip_expire        AS yd_vip_expire
    FROM dim.dim_account_df
    WHERE dt = DATE_SUB('${bizdate}', 1)
),

-- ============================================================
-- 3. 今日成功的VIP订单（从dwd_order_vip_di获取）
-- ============================================================
today_vip_order_ranked AS (
    SELECT
        mid,
        new_vip_expire_ts,
        plan_duration_days,
        pay_end_ts,
        ROW_NUMBER() OVER (
            PARTITION BY mid
            ORDER BY pay_end_ts DESC
        ) AS rn
    FROM dwd.dwd_order_vip_di
    WHERE dt = '${bizdate}'
      AND order_status = 'success'
      AND mid IS NOT NULL
      AND new_vip_expire_ts IS NOT NULL
),

today_vip_latest AS (
    SELECT
        mid,
        new_vip_expire_ts,
        plan_duration_days
    FROM today_vip_order_ranked
    WHERE rn = 1
),

-- ============================================================
-- 4. 今日主动行为增量
-- ============================================================
today_follow_action AS (
    SELECT
        mid,
        -- COLLECT_SET 自动忽略 NULL
        COLLECT_SET(IF(event_id = 'social_follow', target_mid, NULL))   AS today_follow,
        COLLECT_SET(IF(event_id = 'social_unfollow', target_mid, NULL)) AS today_unfollow,
        COLLECT_SET(IF(event_id = 'social_block', target_mid, NULL))    AS today_block,
        COLLECT_SET(IF(event_id = 'social_unblock', target_mid, NULL))  AS today_unblock
    FROM dwd.dwd_action_account_di
    WHERE dt = '${bizdate}'
      AND mid IS NOT NULL
      AND target_mid IS NOT NULL
    GROUP BY mid
),

-- ============================================================
-- 5. 今日被动行为增量
-- ============================================================
today_passive_action AS (
    SELECT
        target_mid                                                          AS mid,
        COLLECT_SET(IF(event_id = 'social_follow', mid, NULL))              AS today_be_followed,
        COLLECT_SET(IF(event_id = 'social_unfollow', mid, NULL))            AS today_be_unfollowed,
        COLLECT_SET(IF(event_id = 'social_block', mid, NULL))               AS today_be_blocked,
        COLLECT_SET(IF(event_id = 'social_unblock', mid, NULL))             AS today_be_unblocked
    FROM dwd.dwd_action_account_di
    WHERE dt = '${bizdate}'
      AND mid IS NOT NULL
      AND target_mid IS NOT NULL
    GROUP BY target_mid
),

-- ============================================================
-- 6. 合并所有用户
-- ============================================================
all_users AS (
    SELECT mid FROM user_profile_latest
    UNION
    SELECT mid FROM yesterday_snapshot
    UNION
    SELECT mid FROM today_follow_action
    UNION
    SELECT mid FROM today_passive_action
    UNION
    SELECT mid FROM today_vip_latest
),

-- ============================================================
-- 7. 计算累计关系（处理NULL安全）
-- ============================================================
relation_calculated AS (
    SELECT
        u.mid,

        -- following = (昨日 ∪ 今日新增) - 今日取关
        COALESCE(
            ARRAY_EXCEPT(
                ARRAY_UNION(
                    COALESCE(y.yd_following_list, array_slice(array(0L),1,0)),
                    COALESCE(f.today_follow, array_slice(array(0L),1,0))
                ),
                COALESCE(f.today_unfollow, array_slice(array(0L),1,0))
            ),
            array_slice(array(0L),1,0)
        ) AS following_list,

        -- follower = (昨日 ∪ 今日新粉) - 今日取关我的
        COALESCE(
            ARRAY_EXCEPT(
                ARRAY_UNION(
                    COALESCE(y.yd_follower_list, array_slice(array(0L),1,0)),
                    COALESCE(p.today_be_followed, array_slice(array(0L),1,0))
                ),
                COALESCE(p.today_be_unfollowed, array_slice(array(0L),1,0))
            ),
            array_slice(array(0L),1,0)
        ) AS follower_list,

        -- blocking = (昨日 ∪ 今日拉黑) - 今日取消拉黑
        COALESCE(
            ARRAY_EXCEPT(
                ARRAY_UNION(
                    COALESCE(y.yd_blocking_list, array_slice(array(0L),1,0)),
                    COALESCE(f.today_block, array_slice(array(0L),1,0))
                ),
                COALESCE(f.today_unblock, array_slice(array(0L),1,0))
            ),
            array_slice(array(0L),1,0)
        ) AS blocking_list,

        -- blocked_by = (昨日 ∪ 今日被拉黑) - 今日被取消拉黑
        COALESCE(
            ARRAY_EXCEPT(
                ARRAY_UNION(
                    COALESCE(y.yd_blocked_by_list, array_slice(array(0L),1,0)),
                    COALESCE(p.today_be_blocked, array_slice(array(0L),1,0))
                ),
                COALESCE(p.today_be_unblocked, array_slice(array(0L),1,0))
            ),
            array_slice(array(0L),1,0)
        ) AS blocked_by_list,

        -- 今日关注变化
        GREATEST(SIZE(f.today_follow), 0)
        - GREATEST(SIZE(f.today_unfollow), 0) AS following_chg,

        -- 今日粉丝变化
        GREATEST(SIZE(p.today_be_followed), 0)
        - GREATEST(SIZE(p.today_be_unfollowed), 0) AS follower_chg

    FROM all_users u
    LEFT JOIN yesterday_snapshot y ON u.mid = y.mid
    LEFT JOIN today_follow_action f ON u.mid = f.mid
    LEFT JOIN today_passive_action p ON u.mid = p.mid
),

-- ============================================================
-- 8. 计算VIP状态
-- ============================================================
vip_calculated AS (
    SELECT
        u.mid,

        -- VIP到期时间：优先用今日新订单，否则沿用昨日
        CASE
            WHEN tv.new_vip_expire_ts IS NOT NULL
            THEN FROM_UNIXTIME(tv.new_vip_expire_ts DIV 1000)
            ELSE y.yd_vip_expire
        END                                                                 AS vip_expire,

        -- VIP类型：根据到期时间判断是否有效，再根据套餐时长推断类型
        CASE
            -- 今日有新成功订单
            WHEN tv.new_vip_expire_ts IS NOT NULL THEN
                CASE
                    -- 判断是否已过期
                    WHEN tv.new_vip_expire_ts > UNIX_TIMESTAMP('${bizdate} 23:59:59') * 1000 THEN
                        -- 根据套餐时长判断月度/年度
                        CASE
                            WHEN tv.plan_duration_days <= 31 THEN 1   -- 月度
                            ELSE 2                                     -- 年度
                        END
                    ELSE 0  -- 新订单但已过期
                END
            -- 无新订单，沿用昨日状态
            WHEN y.yd_vip_expire IS NOT NULL
                 AND UNIX_TIMESTAMP(y.yd_vip_expire) > UNIX_TIMESTAMP('${bizdate} 23:59:59')
            THEN y.yd_vip_type
            -- 无VIP或已过期
            ELSE 0
        END                                                                 AS vip_type

    FROM all_users u
    LEFT JOIN yesterday_snapshot y ON u.mid = y.mid
    LEFT JOIN today_vip_latest tv ON u.mid = tv.mid
)

INSERT OVERWRITE TABLE dim.dim_account_df PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 用户基础信息 ==========
    u.mid                                                                       AS mid,
    pr.nick_name                                                                AS nick_name,
    pr.sex                                                                      AS sex,
    pr.face_url                                                                 AS face_url,
    pr.sign                                                                     AS sign,
    pr.level                                                                    AS level,
    pr.birthday                                                                 AS birthday,

    CASE
        WHEN pr.birthday IS NOT NULL
             AND pr.birthday != ''
             AND pr.birthday RLIKE '^\\d{4}-\\d{2}-\\d{2}$'
        THEN CAST(FLOOR(DATEDIFF('${bizdate}', pr.birthday) / 365.25) AS INT)
        ELSE NULL
    END                                                                         AS age,
    CASE
        WHEN pr.birthday IS NOT NULL
             AND pr.birthday != ''
             AND pr.birthday RLIKE '^\\d{4}-\\d{2}-\\d{2}$'
        THEN CAST(SUBSTR(pr.birthday, 1, 4) AS INT)
        ELSE NULL
    END                                                                         AS birth_year,

    -- ========== 账号状态（VIP信息从dwd_order_vip_di获取）==========
    pr.coins                                                                    AS coins,
    v.vip_type                                                                  AS vip_type,
    CASE v.vip_type
        WHEN 0 THEN '普通用户'
        WHEN 1 THEN '月度大会员'
        WHEN 2 THEN '年度大会员'
        ELSE '未知'
    END                                                                         AS vip_type_name,
    v.vip_expire                                                              AS vip_expire,
    pr.status                                                                   AS status,
    CASE pr.status
        WHEN 0 THEN '正常'
        WHEN 1 THEN '封禁'
        WHEN 2 THEN '注销'
        ELSE '未知'
    END                                                                         AS status_name,

    -- ========== 认证信息 ==========
    COALESCE(pr.official_type, -1)                                              AS official_type,
    pr.official_desc                                                            AS official_desc,
    IF(pr.official_type IS NOT NULL AND pr.official_type >= 0, TRUE, FALSE)     AS is_official,

    -- ========== 用户设置 ==========
    COALESCE(pr.privacy_show_fav, FALSE)                                        AS privacy_show_fav,
    COALESCE(pr.privacy_show_history, FALSE)                                    AS privacy_show_history,
    COALESCE(pr.push_comment, TRUE)                                             AS push_comment,
    COALESCE(pr.push_like, TRUE)                                                AS push_like,
    COALESCE(pr.push_at, TRUE)                                                  AS push_at,
    pr.theme                                                                    AS theme,

    -- ========== 用户标签 ==========
    COALESCE(pr.tags_array, ARRAY())                                            AS tags,
    SIZE(COALESCE(pr.tags_array, ARRAY()))                                      AS tag_cnt,
    CASE
        WHEN SIZE(COALESCE(pr.tags_array, ARRAY())) > 0 THEN pr.tags_array[0]
        ELSE NULL
    END                                                                         AS primary_tag,

    -- ========== 账号时间信息 ==========
    CASE
        WHEN pr.created_at_ts IS NOT NULL
        THEN FROM_UNIXTIME(pr.created_at_ts DIV 1000)
        ELSE NULL
    END                                                                         AS created_at,
    CASE
        WHEN pr.updated_at_ts IS NOT NULL
        THEN FROM_UNIXTIME(pr.updated_at_ts DIV 1000)
        ELSE NULL
    END                                                                         AS updated_at,
    CASE
        WHEN pr.created_at_ts IS NOT NULL
        THEN CAST(DATEDIFF('${bizdate}', FROM_UNIXTIME(pr.created_at_ts DIV 1000)) AS INT)
        ELSE NULL
    END                                                                         AS account_age_days,

    -- ========== 累计关注关系 ==========
    r.following_list                                                            AS following_list,
    SIZE(COALESCE(r.following_list, array_slice(array(0L),1,0)))                AS following_cnt,
    r.follower_list                                                             AS follower_list,
    SIZE(COALESCE(r.follower_list, array_slice(array(0L),1,0)))                 AS follower_cnt,

    ARRAY_INTERSECT(r.following_list, r.follower_list)                          AS mutual_follow_list,
    SIZE(COALESCE(ARRAY_INTERSECT(r.following_list, r.follower_list), array_slice(array(0L),1,0))) AS mutual_follow_cnt,

    -- ========== 累计拉黑关系 ==========
    r.blocking_list                                                             AS blocking_list,
    SIZE(COALESCE(r.blocking_list, array_slice(array(0L),1,0)))                 AS blocking_cnt,
    r.blocked_by_list                                                           AS blocked_by_list,
    SIZE(COALESCE(r.blocked_by_list, array_slice(array(0L),1,0)))               AS blocked_by_cnt,

    -- ========== 关系变化趋势 ==========
    r.following_chg                                                             AS following_chg,
    r.follower_chg                                                              AS follower_chg,

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP()                                                         AS dw_create_time

FROM all_users u
LEFT JOIN user_profile_latest pr ON u.mid = pr.mid
LEFT JOIN relation_calculated r ON u.mid = r.mid
LEFT JOIN vip_calculated v ON u.mid = v.mid
WHERE u.mid IS NOT NULL;
