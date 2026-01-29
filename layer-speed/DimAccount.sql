DROP TABLE IF EXISTS kafka_user_profile;
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dim-account',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

DROP TABLE IF EXISTS kafka_event_social;
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dim-account-social',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

DROP TABLE IF EXISTS hbase_dim_account;
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
        -- vip_expire STRING,
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
    'zookeeper.quorum' = 'hbase-master:2181',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s'
);


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
            THEN CAST(TIMESTAMPDIFF(YEAR, TO_DATE(birthday), CURRENT_DATE) AS STRING)
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
        -- 修复点：使用 CAST 将 ARRAY 转为 STRING
        COALESCE(CAST(tags AS STRING), '[]'),
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
            THEN CAST(TIMESTAMPDIFF(DAY, TO_DATE(DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at, 3), 'yyyy-MM-dd')), CURRENT_DATE) AS STRING)
            ELSE NULL
        END,
        DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss')
    )
FROM kafka_user_profile
WHERE mid IS NOT NULL;
