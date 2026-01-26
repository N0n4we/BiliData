-- ============================================================
-- DimComment FlinkSQL - 实时消费Kafka写入HBase
-- 数据源：
--   1. app_comment - 评论基础数据
--   2. app_event_comment - 评论行为埋点（点赞/点踩/回复等）
-- 写入：dim:dim_comment
--
-- 注意：离线层的 like_count/dislike_count/reply_count 是累计值
--       实时层写入的是增量值，会在同一字段上累加
--       批处理层每日会重新计算正确的累计值
-- ============================================================

-- 1. 创建 Kafka Source 表 - 评论基础数据
CREATE TABLE kafka_comment (
    rpid                BIGINT,
    oid                 STRING,
    otype               INT,
    mid                 BIGINT,
    root                BIGINT,
    parent              BIGINT,
    content             STRING,
    like_count          INT,
    dislike_count       INT,
    reply_count         INT,
    state               INT,
    created_at          BIGINT,
    updated_at          BIGINT,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'app_comment',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = 'flink-dim-comment',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 创建 Kafka Source 表 - 评论行为埋点
CREATE TABLE kafka_event_comment (
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
    'properties.group.id' = 'flink-dim-comment-event',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 3. 创建 HBase Sink 表 - 评论基础信息
CREATE TABLE hbase_dim_comment (
    rowkey STRING,
    info ROW<
        rpid STRING,
        oid STRING,
        otype STRING,
        mid STRING,
        root STRING,
        parent STRING,
        is_root STRING,
        state STRING
    >,
    content ROW<
        content STRING
    >,
    meta ROW<
        created_at STRING,
        updated_at STRING,
        created_date STRING,
        updated_date STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_comment',
    'zookeeper.quorum' = '${hbase.zookeeper.quorum}'
);

-- 4. 创建 HBase Sink 表 - 评论统计增量
-- 注意：实时层写入的是增量值，批处理层会重新计算累计值
CREATE TABLE hbase_dim_comment_stats (
    rowkey STRING,
    stats ROW<
        like_count STRING,
        dislike_count STRING,
        reply_count STRING
    >,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'dim:dim_comment',
    'zookeeper.quorum' = '${hbase.zookeeper.quorum}'
);

-- ============================================================
-- 5. 插入评论基础数据
-- ============================================================
INSERT INTO hbase_dim_comment
SELECT
    REVERSE(CAST(rpid AS STRING)) AS rowkey,
    ROW(
        CAST(rpid AS STRING),
        oid,
        CAST(otype AS STRING),
        CAST(mid AS STRING),
        CAST(root AS STRING),
        CAST(parent AS STRING),
        CASE WHEN root = 0 THEN 'true' ELSE 'false' END,
        CAST(state AS STRING)
    ),
    ROW(
        content
    ),
    ROW(
        CAST(created_at AS STRING),
        CAST(updated_at AS STRING),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at, 3), 'yyyy-MM-dd'),
        DATE_FORMAT(TO_TIMESTAMP_LTZ(updated_at, 3), 'yyyy-MM-dd')
    )
FROM kafka_comment
WHERE rpid IS NOT NULL AND oid IS NOT NULL;

-- ============================================================
-- 6. 实时聚合评论行为，计算统计增量
-- 使用滚动窗口聚合，每10秒输出一次
-- 事件类型：comment_like, comment_unlike, comment_dislike, comment_undislike, comment_reply
--
-- 注意：这里写入的是增量值（净变化量）
--       like_count = 点赞数 - 取消点赞数（净增量）
--       dislike_count = 点踩数 - 取消点踩数（净增量）
--       reply_count = 回复数（增量）
-- ============================================================
INSERT INTO hbase_dim_comment_stats
SELECT
    REVERSE(CAST(rpid AS STRING)) AS rowkey,
    ROW(
        -- like_count: 点赞净增量
        CAST(SUM(CASE
            WHEN event_id = 'comment_like' THEN 1
            WHEN event_id = 'comment_unlike' THEN -1
            ELSE 0
        END) AS STRING),
        -- dislike_count: 点踩净增量
        CAST(SUM(CASE
            WHEN event_id = 'comment_dislike' THEN 1
            WHEN event_id = 'comment_undislike' THEN -1
            ELSE 0
        END) AS STRING),
        -- reply_count: 回复增量
        CAST(SUM(CASE WHEN event_id = 'comment_reply' THEN 1 ELSE 0 END) AS STRING)
    )
FROM kafka_event_comment
WHERE rpid IS NOT NULL
  AND event_id IN ('comment_like', 'comment_unlike', 'comment_dislike', 'comment_undislike', 'comment_reply')
GROUP BY rpid, TUMBLE(proc_time, INTERVAL '10' SECOND);
