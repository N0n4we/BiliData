-- ============================================================
-- DimAccount FlinkSQL - 实时消费Kafka写入HBase
-- 数据源：
--   1. app_user_profile - 用户基础数据
--   2. app_event_social - 社交行为埋点（关注/取关/拉黑等）
-- 写入：dim:dim_account
--
-- 注意：实时层只写入增量变化值到 following_chg/follower_chg 字段
--       累计值 following_cnt/follower_cnt 由批处理层计算
-- ============================================================

-- 1. 创建 Kafka Source 表 - 用户基础数据
CREATE TABLE kafka_user_profile (
    mid                 BIGINT,
    nick_name           STRING,
    sex                 STRING,
    face_url            STRING,
    sign                STRING,
    `level`             INT,
    birthday            STRING,
    coins               DECIMAL(12,2),
    vip_type            INT,
    official_verify     ROW<`type` INT, `desc` STRING>,
    settings            ROW<
        privacy ROW<show_fav BOOLEAN, show_history BOOLEAN>,
        push ROW<`comment` BOOLEAN, `like` BOOLEAN, `at` BOOLEAN>,
        theme STRING
    >,
    tags                ARRAY<STRING>,
    `status`            INT,
    created_at          BIGINT,
    updated_at          BIGINT,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_user_profile',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-account',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 Kafka Source 表 - 社交行为埋点
CREATE TABLE kafka_event_social (
    event_id            STRING,
    mid                 BIGINT,
    target_mid          BIGINT,
    server_ts           BIGINT,
    properties          ROW<
        event_type STRING,
        target_mid BIGINT,
        `from` STRING,
        is_mutual BOOLEAN
    >,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_social',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-account-social',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 3. 创建 HBase Sink 表 - 用户基础信息
CREATE TABLE hbase_dim_account (
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

-- 4. 创建 HBase Sink 表 - 用户关系变化（只写入变化量字段）
-- 注意：following_cnt/follower_cnt 等累计字段由批处理层写入
--       实时层只写入 following_chg/follower_chg 变化量
CREATE TABLE hbase_dim_account_relation (
    rowkey STRING,
    relation ROW<
        following_chg STRING,
        follower_chg STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_account',
    'zookeeper.quorum' = 'zookeeper:2181'
);

-- ============================================================
-- 5. 插入用户基础数据
-- ============================================================
INSERT INTO hbase_dim_account
SELECT
    REVERSE(CAST(mid AS STRING)) AS rowkey,
    ROW(
        CAST(mid AS STRING),
        nick_name,
        sex,
        face_url,
        sign,
        CAST(`level` AS STRING),
        birthday,
        CASE
            WHEN birthday IS NOT NULL AND birthday <> ''
            THEN CAST(FLOOR(DATEDIFF(CURRENT_DATE, TO_DATE(birthday)) / 365.25) AS STRING)
            ELSE NULL
        END,
        CASE
            WHEN birthday IS NOT NULL AND birthday <> ''
            THEN SUBSTRING(birthday, 1, 4)
            ELSE NULL
        END
    ),
    ROW(
        CAST(coins AS STRING),
        CAST(vip_type AS STRING),
        CASE vip_type
            WHEN 0 THEN '普通用户'
            WHEN 1 THEN '月度大会员'
            WHEN 2 THEN '年度大会员'
            ELSE '未知'
        END,
        NULL,  -- vip_expire 由批处理层从订单数据计算
        CAST(`status` AS STRING),
        CASE `status`
            WHEN 0 THEN '正常'
            WHEN 1 THEN '封禁'
            WHEN 2 THEN '注销'
            ELSE '未知'
        END
    ),
    ROW(
        CAST(COALESCE(official_verify.`type`, -1) AS STRING),
        official_verify.`desc`,
        CASE WHEN official_verify.`type` IS NOT NULL AND official_verify.`type` >= 0 THEN 'true' ELSE 'false' END
    ),
    ROW(
        CAST(COALESCE(settings.privacy.show_fav, FALSE) AS STRING),
        CAST(COALESCE(settings.privacy.show_history, FALSE) AS STRING),
        CAST(COALESCE(settings.push.`comment`, TRUE) AS STRING),
        CAST(COALESCE(settings.push.`like`, TRUE) AS STRING),
        CAST(COALESCE(settings.push.`at`, TRUE) AS STRING),
        settings.theme
    ),
    ROW(
        CASE
            WHEN tags IS NOT NULL AND CARDINALITY(tags) > 0
            THEN CONCAT('[', ARRAY_JOIN(tags, ','), ']')
            ELSE '[]'
        END,
        CAST(COALESCE(CARDINALITY(tags), 0) AS STRING),
        CASE
            WHEN tags IS NOT NULL AND CARDINALITY(tags) > 0
            THEN tags[1]
            ELSE NULL
        END
    ),
    ROW(
        DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at, 3), 'yyyy-MM-dd HH:mm:ss'),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(updated_at, 3), 'yyyy-MM-dd HH:mm:ss'),
        CASE
            WHEN created_at IS NOT NULL
            THEN CAST(DATEDIFF(CURRENT_DATE, TO_DATE(DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at, 3), 'yyyy-MM-dd'))) AS STRING)
            ELSE NULL
        END,
        DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss')
    )
FROM kafka_user_profile
WHERE mid IS NOT NULL;

-- ============================================================
-- 6. 实时聚合社交行为（主动行为），计算关注变化量
-- 使用滚动窗口聚合，每10秒输出一次
-- 只写入 following_chg 字段
-- ============================================================
INSERT INTO hbase_dim_account_relation
SELECT
    REVERSE(CAST(mid AS STRING)) AS rowkey,
    ROW(
        -- following_chg: 关注变化量 = 关注数 - 取关数
        CAST(SUM(CASE WHEN event_id = 'social_follow' THEN 1 WHEN event_id = 'social_unfollow' THEN -1 ELSE 0 END) AS STRING),
        -- follower_chg: 此处为0，被动关系在下一个INSERT处理
        '0'
    )
FROM kafka_event_social
WHERE mid IS NOT NULL
  AND event_id IN ('social_follow', 'social_unfollow')
GROUP BY mid, TUMBLE(proc_time, INTERVAL '10' SECOND);

-- ============================================================
-- 7. 实时聚合被动关系（被关注），计算粉丝变化量
-- 按 target_mid 聚合，写入 follower_chg 字段
-- ============================================================
INSERT INTO hbase_dim_account_relation
SELECT
    REVERSE(CAST(target_mid AS STRING)) AS rowkey,
    ROW(
        -- following_chg: 此处为0
        '0',
        -- follower_chg: 粉丝变化量 = 被关注数 - 被取关数
        CAST(SUM(CASE WHEN event_id = 'social_follow' THEN 1 WHEN event_id = 'social_unfollow' THEN -1 ELSE 0 END) AS STRING)
    )
FROM kafka_event_social
WHERE target_mid IS NOT NULL
  AND event_id IN ('social_follow', 'social_unfollow')
GROUP BY target_mid, TUMBLE(proc_time, INTERVAL '10' SECOND);
