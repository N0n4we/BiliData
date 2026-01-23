-- 切换到 dwd 库
USE CATALOG hive_prod;
CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

-- 创建视频度量值每日汇总表
CREATE TABLE IF NOT EXISTS dwd.dwd_stats_video_di (
    bvid                    STRING      COMMENT '视频BV号',

    -- ========== 观看指标 ==========
    view_count              INT         COMMENT '观看次数',
    view_user_count         INT         COMMENT '观看用户数(UV)',
    total_play_duration_sec BIGINT      COMMENT '总播放时长(秒)',
    avg_play_duration_sec   INT         COMMENT '平均播放时长(秒)',

    -- ========== 互动指标 ==========
    like_count_delta        INT         COMMENT '点赞增量',
    unlike_count_delta      INT         COMMENT '取消点赞增量',
    net_like_delta          INT         COMMENT '净点赞增量(点赞-取消)',

    coin_count_delta        INT         COMMENT '投币次数增量',
    coin_total_delta        INT         COMMENT '投币总数增量',

    favorite_count_delta    INT         COMMENT '收藏增量',
    unfavorite_count_delta  INT         COMMENT '取消收藏增量',
    net_favorite_delta      INT         COMMENT '净收藏增量',

    share_count             INT         COMMENT '分享次数',
    danmaku_count           INT         COMMENT '弹幕数',
    triple_count            INT         COMMENT '一键三连次数'
)
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
