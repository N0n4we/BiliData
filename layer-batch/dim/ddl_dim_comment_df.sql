CREATE TABLE IF NOT EXISTS dim_comment_df (
    rpid            BIGINT      COMMENT '评论ID',
    oid             STRING      COMMENT '视频BV号',
    otype           INT         COMMENT '对象类型：1=视频',
    mid             BIGINT      COMMENT '发表者用户ID',
    root            BIGINT      COMMENT '根评论ID，0表示是根评论',
    parent          BIGINT      COMMENT '父评论ID',
    content         STRING      COMMENT '评论内容',
    like_count      INT         COMMENT '点赞数',
    dislike_count   INT         COMMENT '点踩数',
    reply_count     INT         COMMENT '回复数',
    state           INT         COMMENT '状态',
    is_root         BOOLEAN     COMMENT '是否根评论',
    created_at      BIGINT      COMMENT '创建时间戳(ms)',
    updated_at      BIGINT      COMMENT '更新时间戳(ms)',
    created_date    STRING      COMMENT '创建日期',
    updated_date    STRING      COMMENT '更新日期'
)
PARTITIONED BY (dt STRING COMMENT '数据快照日期')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
