-- ============================================================
-- DwsVideoStatsAccount FlinkSQL - 实时消费Kafka写入ClickHouse
-- 数据源：
--   1. app_event_video - 视频行为埋点（观看/点赞/投币/收藏/分享/弹幕/三连等）
-- 写入：dws_video_stats_account_di
--
-- 处理语义：
--   以UP主mid为粒度聚合视频统计数据
--   注：实时层不lookup维度表，维度字段填null，由离线层T+1覆盖补全
--   使用CUMULATE窗口实现天内累加，每分钟输出当天累计值
-- ============================================================

-- 设置时区，确保CUMULATE窗口从北京时间00:00开始
SET 'table.local-time-zone' = 'Asia/Shanghai';

-- 1. 创建 Kafka Source 表 - 视频行为埋点
DROP TABLE IF EXISTS kafka_event_video;
CREATE TABLE kafka_event_video (
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
    'properties.group.id' = 'flink-dws-video-stats-account',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 ClickHouse Sink 表 - UP主视频统计汇总表
-- 字段与batch层/ClickHouse DDL保持一致，维度字段填null
DROP TABLE IF EXISTS clickhouse_dws_video_stats_account;
CREATE TABLE clickhouse_dws_video_stats_account (
    -- 账号维度信息（实时层填null，由离线层补全）
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

    -- 视频统计汇总
    video_cnt                   BIGINT,

    -- 观看指标汇总
    total_view_count            BIGINT,
    total_view_user_count       BIGINT,
    total_play_duration_sec     BIGINT,
    avg_play_duration_sec       INT,

    -- 互动指标汇总
    total_like_delta            BIGINT,
    total_unlike_delta          BIGINT,
    total_net_like_delta        BIGINT,

    total_coin_count_delta      BIGINT,
    total_coin_total_delta      BIGINT,

    total_favorite_delta        BIGINT,
    total_unfavorite_delta      BIGINT,
    total_net_favorite_delta    BIGINT,

    total_share_count           BIGINT,
    total_danmaku_count         BIGINT,
    total_triple_count          BIGINT,

    -- ETL信息
    dw_create_time              TIMESTAMP(3),
    dt                          STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_video_stats_account_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '10',
    'sink.buffer-flush.interval' = '1s'
);

-- ============================================================
-- 3. 实时聚合视频统计数据，按UP主mid汇总
-- 使用CUMULATE窗口，每1分钟输出当天从00:00到当前的累计值
-- 处理语义：
--   - 按UP主mid聚合视频行为统计
--   - 维度字段填null，由离线层T+1覆盖补全
--   - video_triple事件同时计入点赞/投币/收藏（与DimVideo.sql口径一致）
-- ============================================================

INSERT INTO clickhouse_dws_video_stats_account
SELECT
    -- 账号维度信息（填null，由离线层补全）
    mid,
    CAST(NULL AS STRING)                            AS nick_name,
    CAST(NULL AS STRING)                            AS sex,
    CAST(NULL AS INT)                               AS `level`,
    CAST(NULL AS INT)                               AS vip_type,
    CAST(NULL AS STRING)                            AS vip_type_name,
    CAST(NULL AS INT)                               AS official_type,
    CAST(NULL AS STRING)                            AS official_desc,
    CAST(NULL AS BOOLEAN)                           AS is_official,
    CAST(NULL AS INT)                               AS follower_cnt,
    CAST(NULL AS INT)                               AS following_cnt,

    -- 视频统计汇总（当天累计值）
    COUNT(DISTINCT bvid)                            AS video_cnt,

    -- 观看指标汇总（当天累计值）
    SUM(CASE WHEN event_id = 'video_view' THEN 1 ELSE 0 END)
                                                    AS total_view_count,
    CAST(NULL AS BIGINT)                            AS total_view_user_count,  -- 实时层无法计算
    SUM(CASE WHEN event_id = 'video_play_heartbeat' THEN 15 ELSE 0 END)
                                                    AS total_play_duration_sec,
    CAST(NULL AS INT)                               AS avg_play_duration_sec,  -- 实时层无法计算

    -- 互动指标汇总（当天累计值，video_triple同时计入点赞/投币/收藏）
    SUM(CASE WHEN event_id IN ('video_like', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS total_like_delta,
    SUM(CASE WHEN event_id = 'video_unlike' THEN 1 ELSE 0 END)
                                                    AS total_unlike_delta,
    SUM(CASE
        WHEN event_id IN ('video_like', 'video_triple') THEN 1
        WHEN event_id = 'video_unlike' THEN -1
        ELSE 0
    END)                                            AS total_net_like_delta,

    SUM(CASE WHEN event_id IN ('video_coin', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS total_coin_count_delta,
    SUM(CASE
        WHEN event_id = 'video_coin' THEN COALESCE(properties.coin_count, 1)
        WHEN event_id = 'video_triple' THEN 2
        ELSE 0
    END)                                            AS total_coin_total_delta,

    SUM(CASE WHEN event_id IN ('video_favorite', 'video_triple') THEN 1 ELSE 0 END)
                                                    AS total_favorite_delta,
    SUM(CASE WHEN event_id = 'video_unfavorite' THEN 1 ELSE 0 END)
                                                    AS total_unfavorite_delta,
    SUM(CASE
        WHEN event_id IN ('video_favorite', 'video_triple') THEN 1
        WHEN event_id = 'video_unfavorite' THEN -1
        ELSE 0
    END)                                            AS total_net_favorite_delta,

    SUM(CASE WHEN event_id = 'video_share' THEN 1 ELSE 0 END)
                                                    AS total_share_count,
    SUM(CASE WHEN event_id = 'video_danmaku' THEN 1 ELSE 0 END)
                                                    AS total_danmaku_count,
    SUM(CASE WHEN event_id = 'video_triple' THEN 1 ELSE 0 END)
                                                    AS total_triple_count,

    -- ETL信息
    CURRENT_TIMESTAMP                               AS dw_create_time,
    DATE_FORMAT(window_end, 'yyyy-MM-dd')           AS dt

FROM TABLE(
    CUMULATE(
        TABLE kafka_event_video,
        DESCRIPTOR(event_time),
        INTERVAL '5' SECOND,
        INTERVAL '1' DAY
    )
)
WHERE mid IS NOT NULL
  AND bvid IS NOT NULL
  AND event_id IN ('video_view', 'video_play_heartbeat', 'video_like', 'video_unlike',
                   'video_coin', 'video_favorite', 'video_unfavorite',
                   'video_share', 'video_danmaku', 'video_triple')
GROUP BY window_start, window_end, mid;
