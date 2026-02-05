SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TEMPORARY TABLE kafka_event_video (
    event_id            STRING,
    mid                 BIGINT,
    bvid                STRING,
    server_ts           BIGINT,
    properties          ROW<
        coin_count INT,
        progress_sec INT,
        `from` STRING
    >,
    proc_time AS PROCTIME(),
    event_time AS TO_TIMESTAMP_LTZ(server_ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_video',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dws-video-stats-account-v2',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE hbase_dim_account (
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
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_account',
    'zookeeper.quorum' = 'hbase-master:2181',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '10000',
    'lookup.partial-cache.expire-after-write' = '1h'
);

CREATE TEMPORARY TABLE hbase_dim_video (
    rowkey STRING,
    basic ROW<
        bvid STRING,
        mid STRING,
        pubdate STRING,
        state STRING,
        attribute STRING,
        is_private STRING
    >,
    content ROW<
        title STRING,
        cover STRING,
        desc_text STRING,
        duration STRING,
        category_id STRING,
        category_name STRING
    >,
    meta ROW<
        created_at STRING,
        updated_at STRING,
        created_date STRING,
        updated_date STRING,
        pub_date STRING,
        upload_ip STRING,
        camera STRING,
        software STRING,
        resolution STRING,
        fps STRING,
        audit_info STRING,
        meta_info STRING,
        stats_json STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_video',
    'zookeeper.quorum' = 'hbase-master:2181',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '50000',
    'lookup.partial-cache.expire-after-write' = '1h'
);

CREATE TEMPORARY TABLE clickhouse_dws_video_stats_account_v2 (
    bvid                        STRING,
    title                       STRING,
    duration                    INT,
    pubdate                     BIGINT,
    pub_date                    STRING,
    category_id                 INT,
    category_name               STRING,

    mid                         BIGINT,
    nick_name                   STRING,
    sex                         STRING,
    `level`                     INT,
    vip_type                    INT,
    vip_type_name               STRING,
    official_type               INT,
    official_desc               STRING,
    is_official                 BOOLEAN,
    follower_cnt                INT,
    following_cnt               INT,

    view_count                  BIGINT,
    view_user_count             BIGINT,
    total_play_duration_sec     BIGINT,
    avg_play_duration_sec       INT,

    like_delta                  BIGINT,
    unlike_delta                BIGINT,
    net_like_delta              BIGINT,

    coin_count_delta            BIGINT,
    coin_total_delta            BIGINT,

    favorite_delta              BIGINT,
    unfavorite_delta            BIGINT,
    net_favorite_delta          BIGINT,

    share_count                 BIGINT,
    danmaku_count               BIGINT,
    triple_count                BIGINT,

    dw_create_time              TIMESTAMP(3),
    dt                          STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_video_stats_account_di_v2',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

CREATE TEMPORARY VIEW enriched_video_events AS
SELECT
    e.event_id,
    e.mid,
    e.bvid,
    e.properties,
    e.event_time,

    -- 视频维度信息（从 HBase lookup）
    dv.content.title                                AS title,
    CAST(dv.content.duration AS INT)                AS duration,
    CAST(dv.basic.pubdate AS BIGINT)                AS pubdate,
    dv.meta.pub_date                                AS pub_date,
    CAST(dv.content.category_id AS INT)             AS category_id,
    dv.content.category_name                        AS category_name,

    -- 账号维度信息（从 HBase lookup）
    da.basic.nick_name                              AS nick_name,
    da.basic.sex                                    AS sex,
    CAST(da.basic.`level` AS INT)                   AS `level`,
    CAST(da.`status`.vip_type AS INT)               AS vip_type,
    da.`status`.vip_type_name                       AS vip_type_name,
    CAST(da.official.official_type AS INT)          AS official_type,
    da.official.official_desc                       AS official_desc,
    CAST(da.official.is_official AS BOOLEAN)        AS is_official,
    CAST(da.relation.follower_cnt AS INT)           AS follower_cnt,
    CAST(da.relation.following_cnt AS INT)          AS following_cnt

FROM kafka_event_video AS e
LEFT JOIN hbase_dim_account FOR SYSTEM_TIME AS OF e.proc_time AS da
    ON REVERSE(CAST(e.mid AS STRING)) = da.rowkey
LEFT JOIN hbase_dim_video FOR SYSTEM_TIME AS OF e.proc_time AS dv
    ON REVERSE(e.bvid) = dv.rowkey
WHERE e.mid IS NOT NULL
  AND e.bvid IS NOT NULL
  AND e.event_id IN (
      'video_view', 'video_play_heartbeat', 'video_like', 'video_unlike',
      'video_coin', 'video_favorite', 'video_unfavorite',
      'video_share', 'video_danmaku', 'video_triple'
  );


INSERT INTO clickhouse_dws_video_stats_account_v2
SELECT
    -- 视频维度信息
    bvid,
    FIRST_VALUE(title)                              AS title,
    FIRST_VALUE(duration)                           AS duration,
    FIRST_VALUE(pubdate)                            AS pubdate,
    FIRST_VALUE(pub_date)                           AS pub_date,
    FIRST_VALUE(category_id)                        AS category_id,
    FIRST_VALUE(category_name)                      AS category_name,

    -- 账号维度信息
    mid,
    FIRST_VALUE(nick_name)                          AS nick_name,
    FIRST_VALUE(sex)                                AS sex,
    FIRST_VALUE(`level`)                            AS `level`,
    FIRST_VALUE(vip_type)                           AS vip_type,
    FIRST_VALUE(vip_type_name)                      AS vip_type_name,
    FIRST_VALUE(official_type)                      AS official_type,
    FIRST_VALUE(official_desc)                      AS official_desc,
    FIRST_VALUE(is_official)                        AS is_official,
    FIRST_VALUE(follower_cnt)                       AS follower_cnt,
    FIRST_VALUE(following_cnt)                      AS following_cnt,

    -- 观看指标
    SUM(CASE WHEN event_id = 'video_view' THEN 1 ELSE 0 END)
                                                    AS view_count,
    CAST(NULL AS BIGINT)                            AS view_user_count,
    SUM(CASE WHEN event_id = 'video_play_heartbeat' THEN 15 ELSE 0 END)
                                                    AS total_play_duration_sec,
    CAST(NULL AS INT)                               AS avg_play_duration_sec,

    -- 点赞指标
    SUM(CASE WHEN event_id IN ('video_like', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS like_delta,
    SUM(CASE WHEN event_id = 'video_unlike' THEN 1 ELSE 0 END)
                                                    AS unlike_delta,
    SUM(CASE
        WHEN event_id IN ('video_like', 'video_triple') THEN 1
        WHEN event_id = 'video_unlike' THEN -1
        ELSE 0
    END)                                            AS net_like_delta,

    -- 投币指标
    SUM(CASE WHEN event_id IN ('video_coin', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS coin_count_delta,
    SUM(CASE
        WHEN event_id = 'video_coin' THEN COALESCE(properties.coin_count, 1)
        WHEN event_id = 'video_triple' THEN 2
        ELSE 0
    END)                                            AS coin_total_delta,

    -- 收藏指标
    SUM(CASE WHEN event_id IN ('video_favorite', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS favorite_delta,
    SUM(CASE WHEN event_id = 'video_unfavorite' THEN 1 ELSE 0 END)
                                                    AS unfavorite_delta,
    SUM(CASE
        WHEN event_id IN ('video_favorite', 'video_triple') THEN 1
        WHEN event_id = 'video_unfavorite' THEN -1
        ELSE 0
    END)                                            AS net_favorite_delta,

    -- 其他指标
    SUM(CASE WHEN event_id = 'video_share' THEN 1 ELSE 0 END)
                                                    AS share_count,
    SUM(CASE WHEN event_id = 'video_danmaku' THEN 1 ELSE 0 END)
                                                    AS danmaku_count,
    SUM(CASE WHEN event_id = 'video_triple' THEN 1 ELSE 0 END)
                                                    AS triple_count,

    -- ETL信息
    CURRENT_TIMESTAMP                               AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd')           AS dt

FROM TABLE(
    CUMULATE(
        TABLE enriched_video_events,
        DESCRIPTOR(event_time),
        INTERVAL '15' SECOND,
        INTERVAL '1' DAY
    )
)
GROUP BY
    window_start,
    window_end,
    bvid,
    mid;
