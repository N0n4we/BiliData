-- ============================================================
-- Hive2HbaseDim - 批处理层将Hive dim表同步到HBase
-- 功能：将Hive中dt=${bizdate}的所有dim_*表upsert到HBase
-- 条件：仅同步updated_at日期 <= bizdate的记录
-- ============================================================

SET 'execution.runtime-mode' = 'batch';

-- ============================================================
-- 1. 创建 Hive Catalog
-- ============================================================
CREATE CATALOG hive_prod WITH (
    'type' = 'hive',
    'hive-conf-dir' = '/opt/hive/conf'
);

USE CATALOG hive_prod;

-- ============================================================
-- 2. 创建 HBase Sink 表 - dim_account
-- rowkey: reverse(mid)
-- ============================================================
CREATE TABLE IF NOT EXISTS hbase_dim_account (
    rowkey STRING,
    basic ROW<
        mid STRING,
        nick_name STRING,
        sex STRING,
        face_url STRING,
        sign STRING,
        `level` STRING,
        birthday STRING,
        age STRING,
        birth_year STRING
    >,
    `status` ROW<
        coins STRING,
        vip_type STRING,
        vip_type_name STRING,
        vip_expire STRING,
        `status` STRING,
        status_name STRING
    >,
    official ROW<
        official_type STRING,
        official_desc STRING,
        is_official STRING
    >,
    setting ROW<
        privacy_show_fav STRING,
        privacy_show_history STRING,
        push_comment STRING,
        push_like STRING,
        push_at STRING,
        theme STRING
    >,
    tag ROW<
        tags STRING,
        tag_cnt STRING,
        primary_tag STRING
    >,
    relation ROW<
        following_list STRING,
        following_cnt STRING,
        follower_list STRING,
        follower_cnt STRING,
        mutual_follow_list STRING,
        mutual_follow_cnt STRING,
        blocking_list STRING,
        blocking_cnt STRING,
        blocked_by_list STRING,
        blocked_by_cnt STRING,
        following_chg STRING,
        follower_chg STRING
    >,
    etl ROW<
        created_at STRING,
        updated_at STRING,
        account_age_days STRING,
        dw_create_time STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_account',
    'zookeeper.quorum' = 'zookeeper:2181'
);

-- ============================================================
-- 3. 创建 HBase Sink 表 - dim_video
-- rowkey: reverse(bvid)
-- ============================================================
CREATE TABLE IF NOT EXISTS hbase_dim_video (
    rowkey STRING,
    basic ROW<
        bvid STRING,
        title STRING,
        cover STRING,
        desc_text STRING,
        duration STRING,
        pubdate STRING,
        mid STRING,
        category_id STRING,
        category_name STRING,
        state STRING,
        attribute STRING,
        is_private STRING
    >,
    meta ROW<
        upload_ip STRING,
        camera STRING,
        software STRING,
        resolution STRING,
        fps STRING
    >,
    stats ROW<
        view_count STRING,
        danmaku_count STRING,
        reply_count STRING,
        favorite_count STRING,
        coin_count STRING,
        share_count STRING,
        like_count STRING
    >,
    time_info ROW<
        created_at STRING,
        updated_at STRING,
        created_date STRING,
        updated_date STRING,
        pub_date STRING
    >,
    extra ROW<
        audit_info STRING,
        meta_info STRING,
        stats_json STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_video',
    'zookeeper.quorum' = 'zookeeper:2181'
);

-- ============================================================
-- 4. 创建 HBase Sink 表 - dim_comment
-- rowkey: reverse(rpid)
-- ============================================================
CREATE TABLE IF NOT EXISTS hbase_dim_comment (
    rowkey STRING,
    basic ROW<
        rpid STRING,
        oid STRING,
        otype STRING,
        mid STRING,
        root STRING,
        parent STRING,
        content STRING
    >,
    stats ROW<
        like_count STRING,
        dislike_count STRING,
        reply_count STRING,
        state STRING,
        is_root STRING
    >,
    time_info ROW<
        created_at STRING,
        updated_at STRING,
        created_date STRING,
        updated_date STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_comment',
    'zookeeper.quorum' = 'zookeeper:2181'
);

-- ============================================================
-- 5. 同步 dim_account_df 到 HBase
-- 条件：dt = ${bizdate} AND DATE(updated_at) <= ${bizdate}
-- 修改：使用 CAST(array AS STRING) 替代 ARRAY_JOIN/TRANSFORM
-- ============================================================
INSERT INTO hbase_dim_account
SELECT
    REVERSE(CAST(mid AS STRING)) AS rowkey,
    -- basic 列族
    ROW(
        CAST(mid AS STRING),
        nick_name,
        sex,
        face_url,
        sign,
        CAST(`level` AS STRING),
        birthday,
        CAST(age AS STRING),
        CAST(birth_year AS STRING)
    ),
    -- status 列族
    ROW(
        CAST(coins AS STRING),
        CAST(vip_type AS STRING),
        vip_type_name,
        CAST(vip_expire AS STRING),
        CAST(`status` AS STRING),
        status_name
    ),
    -- official 列族
    ROW(
        CAST(official_type AS STRING),
        official_desc,
        CAST(is_official AS STRING)
    ),
    -- setting 列族
    ROW(
        CAST(privacy_show_fav AS STRING),
        CAST(privacy_show_history AS STRING),
        CAST(push_comment AS STRING),
        CAST(push_like AS STRING),
        CAST(push_at AS STRING),
        theme
    ),
    -- tag 列族 (使用 CAST 替代 ARRAY_JOIN)
    ROW(
        IF(tags IS NULL OR CARDINALITY(tags) = 0, '[]', CAST(tags AS STRING)),
        CAST(tag_cnt AS STRING),
        primary_tag
    ),
    -- relation 列族 (使用 CAST 替代 TRANSFORM + ARRAY_JOIN)
    ROW(
        IF(following_list IS NULL OR CARDINALITY(following_list) = 0, '[]', CAST(following_list AS STRING)),
        CAST(following_cnt AS STRING),
        IF(follower_list IS NULL OR CARDINALITY(follower_list) = 0, '[]', CAST(follower_list AS STRING)),
        CAST(follower_cnt AS STRING),
        IF(mutual_follow_list IS NULL OR CARDINALITY(mutual_follow_list) = 0, '[]', CAST(mutual_follow_list AS STRING)),
        CAST(mutual_follow_cnt AS STRING),
        IF(blocking_list IS NULL OR CARDINALITY(blocking_list) = 0, '[]', CAST(blocking_list AS STRING)),
        CAST(blocking_cnt AS STRING),
        IF(blocked_by_list IS NULL OR CARDINALITY(blocked_by_list) = 0, '[]', CAST(blocked_by_list AS STRING)),
        CAST(blocked_by_cnt AS STRING),
        CAST(following_chg AS STRING),
        CAST(follower_chg AS STRING)
    ),
    -- etl 列族
    ROW(
        DATE_FORMAT(created_at, 'yyyy-MM-dd HH:mm:ss'),
        DATE_FORMAT(updated_at, 'yyyy-MM-dd HH:mm:ss'),
        CAST(account_age_days AS STRING),
        DATE_FORMAT(dw_create_time, 'yyyy-MM-dd HH:mm:ss')
    )
FROM dim.dim_account_df
WHERE dt = '${bizdate}'
  AND DATE_FORMAT(updated_at, 'yyyy-MM-dd') <= '${bizdate}';

-- ============================================================
-- 6. 同步 dim_video_df 到 HBase
-- 条件：dt = ${bizdate} AND updated_date <= ${bizdate}
-- ============================================================
INSERT INTO hbase_dim_video
SELECT
    REVERSE(bvid) AS rowkey,
    -- basic 列族
    ROW(
        bvid,
        title,
        cover,
        desc_text,
        CAST(duration AS STRING),
        CAST(pubdate AS STRING),
        CAST(mid AS STRING),
        CAST(category_id AS STRING),
        category_name,
        CAST(state AS STRING),
        CAST(attribute AS STRING),
        CAST(is_private AS STRING)
    ),
    -- meta 列族
    ROW(
        upload_ip,
        camera,
        software,
        resolution,
        CAST(fps AS STRING)
    ),
    -- stats 列族
    ROW(
        CAST(view_count AS STRING),
        CAST(danmaku_count AS STRING),
        CAST(reply_count AS STRING),
        CAST(favorite_count AS STRING),
        CAST(coin_count AS STRING),
        CAST(share_count AS STRING),
        CAST(like_count AS STRING)
    ),
    -- time_info 列族
    ROW(
        CAST(created_at AS STRING),
        CAST(updated_at AS STRING),
        created_date,
        updated_date,
        pub_date
    ),
    -- extra 列族
    ROW(
        audit_info,
        meta_info,
        stats
    )
FROM dim.dim_video_df
WHERE dt = '${bizdate}'
  AND updated_date <= '${bizdate}';

-- ============================================================
-- 7. 同步 dim_comment_df 到 HBase
-- 条件：dt = ${bizdate} AND updated_date <= ${bizdate}
-- ============================================================
INSERT INTO hbase_dim_comment
SELECT
    REVERSE(CAST(rpid AS STRING)) AS rowkey,
    -- basic 列族
    ROW(
        CAST(rpid AS STRING),
        oid,
        CAST(otype AS STRING),
        CAST(mid AS STRING),
        CAST(root AS STRING),
        CAST(parent AS STRING),
        content
    ),
    -- stats 列族
    ROW(
        CAST(like_count AS STRING),
        CAST(dislike_count AS STRING),
        CAST(reply_count AS STRING),
        CAST(state AS STRING),
        CAST(is_root AS STRING)
    ),
    -- time_info 列族
    ROW(
        CAST(created_at AS STRING),
        CAST(updated_at AS STRING),
        created_date,
        updated_date
    )
FROM dim.dim_comment_df
WHERE dt = '${bizdate}'
  AND updated_date <= '${bizdate}';
