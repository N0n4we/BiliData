-- ============================================================
-- DimVideo FlinkSQL - 实时消费Kafka写入HBase
-- 数据源：
--   1. app_video_content - 视频基础数据
--   2. app_event_video - 视频行为埋点（观看/点赞/投币/收藏/分享/弹幕等）
--   3. app_event_comment - 评论行为埋点（用于统计视频评论数）
-- 写入：dim:dim_video
--
-- 注意：离线层的统计值是累计值（昨日累计 + 今日增量）
--       实时层写入的是增量值
--       批处理层每日会重新计算正确的累计值
-- ============================================================

-- 1. 创建 Kafka Source 表 - 视频基础数据
CREATE TABLE kafka_video_content (
    bvid                STRING,
    title               STRING,
    cover               STRING,
    desc_text           STRING,
    duration            INT,
    pubdate             BIGINT,
    mid                 BIGINT,
    category_id         INT,
    category_name       STRING,
    state               INT,
    attribute           INT,
    is_private          BOOLEAN,
    meta_info           ROW<
        upload_ip STRING,
        camera STRING,
        software STRING,
        resolution STRING,
        fps INT
    >,
    stats               ROW<
        `view` BIGINT,
        danmaku INT,
        reply INT,
        favorite INT,
        coin INT,
        share INT,
        `like` INT
    >,
    audit_info          STRING,
    created_at          BIGINT,
    updated_at          BIGINT,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_video_content',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-video',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 Kafka Source 表 - 视频行为埋点
CREATE TABLE kafka_event_video (
    event_id            STRING,
    mid                 BIGINT,
    bvid                STRING,
    server_ts           BIGINT,
    properties          ROW<
        coin_count INT,
        `from` STRING
    >,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_video',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-video-event',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 3. 创建 Kafka Source 表 - 评论行为埋点（用于统计视频评论数）
CREATE TABLE kafka_event_comment_for_video (
    event_id            STRING,
    mid                 BIGINT,
    oid                 STRING,
    rpid                BIGINT,
    server_ts           BIGINT,
    properties          ROW<
        otype INT,
        root_rpid BIGINT,
        parent_rpid BIGINT,
        content STRING,
        content_length INT
    >,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_event_comment',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-video-comment',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 4. 创建 HBase Sink 表 - 视频基础信息
CREATE TABLE hbase_dim_video (
    rowkey STRING,
    basic ROW<
        bvid STRING,
        title STRING,
        cover STRING,
        duration STRING,
        pubdate STRING,
        mid STRING,
        category_id STRING,
        category_name STRING,
        state STRING,
        attribute STRING,
        is_private STRING
    >,
    content ROW<
        desc_text STRING,
        upload_ip STRING,
        camera STRING,
        software STRING,
        resolution STRING,
        fps STRING
    >,
    meta ROW<
        created_at STRING,
        updated_at STRING,
        created_date STRING,
        updated_date STRING,
        pub_date STRING,
        audit_info STRING,
        meta_info STRING,
        stats STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_video',
    'zookeeper.quorum' = '${hbase.zookeeper.quorum}'
);

-- 5. 创建 HBase Sink 表 - 视频统计增量（不含评论数）
-- 注意：实时层写入的是增量值，批处理层会重新计算累计值
CREATE TABLE hbase_dim_video_stats (
    rowkey STRING,
    stats ROW<
        view_count STRING,
        danmaku_count STRING,
        favorite_count STRING,
        coin_count STRING,
        share_count STRING,
        like_count STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_video',
    'zookeeper.quorum' = '${hbase.zookeeper.quorum}'
);

-- 6. 创建 HBase Sink 表 - 视频评论数增量
CREATE TABLE hbase_dim_video_reply_stats (
    rowkey STRING,
    stats ROW<
        reply_count STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_video',
    'zookeeper.quorum' = '${hbase.zookeeper.quorum}'
);

-- ============================================================
-- 7. 插入视频基础数据
-- ============================================================
INSERT INTO hbase_dim_video
SELECT
    REVERSE(bvid) AS rowkey,
    ROW(
        bvid,
        title,
        cover,
        CAST(duration AS STRING),
        CAST(pubdate AS STRING),
        CAST(mid AS STRING),
        CAST(category_id AS STRING),
        category_name,
        CAST(state AS STRING),
        CAST(attribute AS STRING),
        CAST(COALESCE(is_private, FALSE) AS STRING)
    ),
    ROW(
        desc_text,
        meta_info.upload_ip,
        meta_info.camera,
        meta_info.software,
        meta_info.resolution,
        CAST(meta_info.fps AS STRING)
    ),
    ROW(
        CAST(created_at AS STRING),
        CAST(updated_at AS STRING),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at, 3), 'yyyy-MM-dd'),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(updated_at, 3), 'yyyy-MM-dd'),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(pubdate, 3), 'yyyy-MM-dd'),
        audit_info,
        CONCAT(
            '{"upload_ip":"', COALESCE(meta_info.upload_ip, ''), '",',
            '"camera":"', COALESCE(meta_info.camera, ''), '",',
            '"software":"', COALESCE(meta_info.software, ''), '",',
            '"resolution":"', COALESCE(meta_info.resolution, ''), '",',
            '"fps":', CAST(COALESCE(meta_info.fps, 0) AS STRING), '}'
        ),
        CONCAT(
            '{"view":', CAST(COALESCE(stats.`view`, 0) AS STRING), ',',
            '"danmaku":', CAST(COALESCE(stats.danmaku, 0) AS STRING), ',',
            '"reply":', CAST(COALESCE(stats.reply, 0) AS STRING), ',',
            '"favorite":', CAST(COALESCE(stats.favorite, 0) AS STRING), ',',
            '"coin":', CAST(COALESCE(stats.coin, 0) AS STRING), ',',
            '"share":', CAST(COALESCE(stats.share, 0) AS STRING), ',',
            '"like":', CAST(COALESCE(stats.`like`, 0) AS STRING), '}'
        )
    )
FROM kafka_video_content
WHERE bvid IS NOT NULL;

-- ============================================================
-- 8. 实时聚合视频行为，计算统计增量
-- 使用滚动窗口聚合，每10秒输出一次
-- 事件类型：video_view, video_like, video_unlike, video_coin,
--          video_favorite, video_unfavorite, video_share, video_danmaku, video_triple
--
-- 注意：这里写入的是增量值（净变化量）
--       view_count = 观看次数（增量）
--       like_count = 点赞数 - 取消点赞数 + 三连（净增量）
--       favorite_count = 收藏数 - 取消收藏数（净增量）
--       coin_count = 投币数（增量，考虑coin_count字段）
--       share_count = 分享数（增量）
--       danmaku_count = 弹幕数（增量）
-- ============================================================
INSERT INTO hbase_dim_video_stats
SELECT
    REVERSE(bvid) AS rowkey,
    ROW(
        -- view_count: 观看增量
        CAST(SUM(CASE WHEN event_id = 'video_view' THEN 1 ELSE 0 END) AS STRING),
        -- danmaku_count: 弹幕增量
        CAST(SUM(CASE WHEN event_id = 'video_danmaku' THEN 1 ELSE 0 END) AS STRING),
        -- favorite_count: 收藏净增量
        CAST(SUM(CASE
            WHEN event_id = 'video_favorite' THEN 1
            WHEN event_id = 'video_unfavorite' THEN -1
            WHEN event_id = 'video_triple' THEN 1
            ELSE 0
        END) AS STRING),
        -- coin_count: 投币增量（考虑coin_count字段，默认为1；三连为2）
        CAST(SUM(CASE
            WHEN event_id = 'video_coin' THEN COALESCE(properties.coin_count, 1)
            WHEN event_id = 'video_triple' THEN 2
            ELSE 0
        END) AS STRING),
        -- share_count: 分享增量
        CAST(SUM(CASE WHEN event_id = 'video_share' THEN 1 ELSE 0 END) AS STRING),
        -- like_count: 点赞净增量（三连也算一个点赞）
        CAST(SUM(CASE
            WHEN event_id = 'video_like' THEN 1
            WHEN event_id = 'video_unlike' THEN -1
            WHEN event_id = 'video_triple' THEN 1
            ELSE 0
        END) AS STRING)
    )
FROM kafka_event_video
WHERE bvid IS NOT NULL
  AND event_id IN ('video_view', 'video_like', 'video_unlike', 'video_coin',
                   'video_favorite', 'video_unfavorite', 'video_share', 'video_danmaku', 'video_triple')
GROUP BY bvid, TUMBLE(proc_time, INTERVAL '10' SECOND);

-- ============================================================
-- 9. 实时聚合评论事件，计算视频评论数增量
-- 使用滚动窗口聚合，每10秒输出一次
-- 事件类型：comment_create（仅统计根评论，otype=1 表示视频评论）
--
-- 注意：这里写入的是增量值
--       reply_count = 评论数（增量）
-- ============================================================
INSERT INTO hbase_dim_video_reply_stats
SELECT
    REVERSE(oid) AS rowkey,
    ROW(
        -- reply_count: 评论增量（仅统计根评论）
        CAST(SUM(CASE WHEN properties.root_rpid = 0 THEN 1 ELSE 0 END) AS STRING)
    )
FROM kafka_event_comment_for_video
WHERE oid IS NOT NULL
  AND event_id = 'comment_create'
  AND properties.otype = 1
GROUP BY oid, TUMBLE(proc_time, INTERVAL '10' SECOND);
