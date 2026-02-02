CREATE TABLE IF NOT EXISTS dws.dws_video_stats_account_di (
    -- ========== 账号维度信息 ==========
    mid                         BIGINT          COMMENT '用户ID',
    nick_name                   STRING          COMMENT '昵称',
    sex                         STRING          COMMENT '性别：男/女/保密',
    level                       INT             COMMENT '等级(0-6)',
    vip_type                    INT             COMMENT 'VIP类型：0=无,1=月度,2=年度',
    vip_type_name               STRING          COMMENT 'VIP类型名称',
    official_type               INT             COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    official_desc               STRING          COMMENT '认证描述',
    is_official                 BOOLEAN         COMMENT '是否认证用户',
    follower_cnt                INT             COMMENT '粉丝数',
    following_cnt               INT             COMMENT '关注数',

    -- ========== 视频统计汇总 ==========
    video_cnt                   BIGINT          COMMENT '当日有数据的视频数',

    -- 观看指标汇总
    total_view_count            BIGINT          COMMENT '总观看次数',
    total_view_user_count       BIGINT          COMMENT '总观看用户数',
    total_play_duration_sec     BIGINT          COMMENT '总播放时长(秒)',
    avg_play_duration_sec       INT             COMMENT '平均播放时长(秒)',

    -- 互动指标汇总
    total_like_delta            BIGINT          COMMENT '总点赞增量',
    total_unlike_delta          BIGINT          COMMENT '总取消点赞增量',
    total_net_like_delta        BIGINT          COMMENT '总净点赞增量',

    total_coin_count_delta      BIGINT          COMMENT '总投币次数增量',
    total_coin_total_delta      BIGINT          COMMENT '总投币数增量',

    total_favorite_delta        BIGINT          COMMENT '总收藏增量',
    total_unfavorite_delta      BIGINT          COMMENT '总取消收藏增量',
    total_net_favorite_delta    BIGINT          COMMENT '总净收藏增量',

    total_share_count           BIGINT          COMMENT '总分享次数',
    total_danmaku_count         BIGINT          COMMENT '总弹幕数',
    total_triple_count          BIGINT          COMMENT '总一键三连次数',

    -- ========== ETL信息 ==========
    dw_create_time              TIMESTAMP       COMMENT '数仓创建时间'
)
COMMENT 'UP主视频统计汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
