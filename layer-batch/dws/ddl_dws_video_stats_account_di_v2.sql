USE CATALOG hive_prod;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

CREATE TABLE IF NOT EXISTS dws.dws_video_stats_account_di_v2 (
    -- ========== 视频维度信息 ==========
    bvid                        STRING          COMMENT '视频BV号',
    title                       STRING          COMMENT '视频标题',
    duration                    INT             COMMENT '视频时长(秒)',
    pubdate                     BIGINT          COMMENT '发布时间戳(ms)',
    pub_date                    STRING          COMMENT '发布日期',
    category_id                 INT             COMMENT '分区ID',
    category_name               STRING          COMMENT '分区名称',

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

    -- ========== 视频统计指标 ==========
    -- 观看指标
    view_count                  BIGINT          COMMENT '观看次数',
    view_user_count             BIGINT          COMMENT '观看用户数',
    total_play_duration_sec     BIGINT          COMMENT '总播放时长(秒)',
    avg_play_duration_sec       INT             COMMENT '平均播放时长(秒)',

    -- 互动指标
    like_delta                  BIGINT          COMMENT '点赞增量',
    unlike_delta                BIGINT          COMMENT '取消点赞增量',
    net_like_delta              BIGINT          COMMENT '净点赞增量',

    coin_count_delta            BIGINT          COMMENT '投币次数增量',
    coin_total_delta            BIGINT          COMMENT '投币数增量',

    favorite_delta              BIGINT          COMMENT '收藏增量',
    unfavorite_delta            BIGINT          COMMENT '取消收藏增量',
    net_favorite_delta          BIGINT          COMMENT '净收藏增量',

    share_count                 BIGINT          COMMENT '分享次数',
    danmaku_count               BIGINT          COMMENT '弹幕数',
    triple_count                BIGINT          COMMENT '一键三连次数',

    -- ========== ETL信息 ==========
    dw_create_time              TIMESTAMP       COMMENT '数仓创建时间'
)
COMMENT 'UP主视频统计明细表（视频粒度）'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
