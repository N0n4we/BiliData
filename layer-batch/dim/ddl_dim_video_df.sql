CREATE TABLE IF NOT EXISTS dim_video_df (
    bvid                STRING      COMMENT '视频BV号',
    title               STRING      COMMENT '视频标题',
    cover               STRING      COMMENT '封面URL',
    desc_text           STRING      COMMENT '视频简介',
    duration            INT         COMMENT '视频时长(秒)',
    pubdate             BIGINT      COMMENT '发布时间戳(ms)',
    mid                 BIGINT      COMMENT 'UP主ID',
    category_id         INT         COMMENT '分区ID',
    category_name       STRING      COMMENT '分区名称',
    state               INT         COMMENT '状态：0=正常',
    attribute           INT         COMMENT '属性位',
    is_private          BOOLEAN     COMMENT '是否私密',

    -- ========== 元信息（展开） ==========
    upload_ip           STRING      COMMENT '上传IP',
    camera              STRING      COMMENT '拍摄设备',
    software            STRING      COMMENT '剪辑软件',
    resolution          STRING      COMMENT '分辨率',
    fps                 INT         COMMENT '帧率',

    -- ========== 统计数据 ==========
    view_count          BIGINT      COMMENT '播放量',
    danmaku_count       INT         COMMENT '弹幕数',
    reply_count         INT         COMMENT '评论数',
    favorite_count      INT         COMMENT '收藏数',
    coin_count          INT         COMMENT '投币数',
    share_count         INT         COMMENT '分享数',
    like_count          INT         COMMENT '点赞数',

    -- ========== 时间信息 ==========
    created_at          BIGINT      COMMENT '创建时间戳(ms)',
    updated_at          BIGINT      COMMENT '更新时间戳(ms)',
    created_date        STRING      COMMENT '创建日期',
    updated_date        STRING      COMMENT '更新日期',
    pub_date            STRING      COMMENT '发布日期',

    -- ========== 审核信息 ==========
    audit_info          STRING      COMMENT '审核记录JSON',

    -- ========== 原始元信息 ==========
    meta_info           STRING      COMMENT '元信息JSON',
    stats               STRING      COMMENT '统计信息JSON'
)
PARTITIONED BY (dt STRING COMMENT '数据快照日期')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
