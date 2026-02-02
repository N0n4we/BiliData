CREATE TEMPORARY TABLE kafka_comment (
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dim-comment',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE kafka_event_comment (
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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-dim-comment-event',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE hbase_dim_comment (
    rowkey STRING,
    info ROW<
        rpid STRING,
        oid STRING,
        otype STRING,
        mid STRING,
        root STRING,
        parent STRING,
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
    'zookeeper.quorum' = 'hbase-master:2181',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s'
);

CREATE TEMPORARY TABLE hbase_dim_comment_incr (
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
    'zookeeper.quorum' = 'hbase-master:2181',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s'
);

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

-- 创建一个视图，将 comment_reply 事件的 rpid 替换为 parent_rpid，其他事件保持原 rpid
CREATE TEMPORARY VIEW normalized_events AS
SELECT
    event_id,
    CASE
        WHEN event_id = 'comment_reply' THEN properties.parent_rpid
        ELSE rpid
    END AS target_rpid,
    proc_time
FROM kafka_event_comment
WHERE rpid IS NOT NULL
  AND event_id IN ('comment_like', 'comment_unlike', 'comment_dislike', 'comment_undislike', 'comment_reply')
  AND (event_id <> 'comment_reply' OR properties.parent_rpid IS NOT NULL);


INSERT INTO hbase_dim_comment_incr
SELECT
    REVERSE(CAST(target_rpid AS STRING)) AS rowkey,
    ROW(
        CAST(SUM(CASE
            WHEN event_id = 'comment_like' THEN 1
            WHEN event_id = 'comment_unlike' THEN -1
            ELSE 0
        END) AS STRING),
        CAST(SUM(CASE
            WHEN event_id = 'comment_dislike' THEN 1
            WHEN event_id = 'comment_undislike' THEN -1
            ELSE 0
        END) AS STRING),
        CAST(SUM(CASE WHEN event_id = 'comment_reply' THEN 1 ELSE 0 END) AS STRING)
    )
FROM TABLE(
    CUMULATE(TABLE normalized_events, DESCRIPTOR(proc_time), INTERVAL '10' SECOND, INTERVAL '1' DAY)
)
GROUP BY
    target_rpid,
    window_start,
    window_end;
