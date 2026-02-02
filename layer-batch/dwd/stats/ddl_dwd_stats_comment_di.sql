CREATE TABLE IF NOT EXISTS dwd.dwd_stats_comment_di (
    rpid                BIGINT      COMMENT '评论ID',
    oid                 STRING      COMMENT '视频BV号',
    like_count_delta    INT         COMMENT '点赞增量',
    unlike_count_delta  INT         COMMENT '取消点赞增量',
    dislike_count_delta INT         COMMENT '点踩增量',
    undislike_count_delta INT       COMMENT '取消点踩增量',
    reply_count_delta   INT         COMMENT '回复增量',
    report_count        INT         COMMENT '举报次数',
    net_like_delta      INT         COMMENT '净点赞增量(点赞-取消)',
    net_dislike_delta   INT         COMMENT '净点踩增量(点踩-取消)'
)
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
