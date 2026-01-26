-- ============================================================
-- ClickHouse DDL - layer-serving 服务层表定义
-- 用于存储从Hive同步过来的dws汇总数据
-- ============================================================

-- ============================================================
-- 1. 创建数据库
-- ============================================================
CREATE DATABASE IF NOT EXISTS dws ON CLUSTER '{cluster}';

-- ============================================================
-- 2. dws_video_stats_account_di - UP主视频统计汇总表
-- 主键：(mid, dt)
-- 分区：按dt日期分区
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.dws_video_stats_account_di ON CLUSTER '{cluster}'
(
    -- 账号维度信息
    mid                         Int64           COMMENT '用户ID',
    nick_name                   String          COMMENT '昵称',
    sex                         String          COMMENT '性别：男/女/保密',
    `level`                     Int32           COMMENT '等级(0-6)',
    vip_type                    Int32           COMMENT 'VIP类型：0=无,1=月度,2=年度',
    vip_type_name               String          COMMENT 'VIP类型名称',
    official_type               Int32           COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    official_desc               String          COMMENT '认证描述',
    is_official                 UInt8           COMMENT '是否认证用户',
    follower_cnt                Int32           COMMENT '粉丝数',
    following_cnt               Int32           COMMENT '关注数',

    -- 视频统计汇总
    video_cnt                   Int64           COMMENT '当日有数据的视频数',

    -- 观看指标汇总
    total_view_count            Int64           COMMENT '总观看次数',
    total_view_user_count       Int64           COMMENT '总观看用户数',
    total_play_duration_sec     Int64           COMMENT '总播放时长(秒)',
    avg_play_duration_sec       Int32           COMMENT '平均播放时长(秒)',

    -- 互动指标汇总
    total_like_delta            Int64           COMMENT '总点赞增量',
    total_unlike_delta          Int64           COMMENT '总取消点赞增量',
    total_net_like_delta        Int64           COMMENT '总净点赞增量',

    total_coin_count_delta      Int64           COMMENT '总投币次数增量',
    total_coin_total_delta      Int64           COMMENT '总投币数增量',

    total_favorite_delta        Int64           COMMENT '总收藏增量',
    total_unfavorite_delta      Int64           COMMENT '总取消收藏增量',
    total_net_favorite_delta    Int64           COMMENT '总净收藏增量',

    total_share_count           Int64           COMMENT '总分享次数',
    total_danmaku_count         Int64           COMMENT '总弹幕数',
    total_triple_count          Int64           COMMENT '总一键三连次数',

    -- ETL信息
    dw_create_time              DateTime64(3)   COMMENT '数仓创建时间',
    dt                          String          COMMENT '日期分区'
)
ENGINE = ReplacingMergeTree(dw_create_time)
PARTITION BY dt
ORDER BY (mid, dt)
SETTINGS index_granularity = 8192;

-- ============================================================
-- 3. dws_account_registry_source_di - 每日新注册账号来源分析表
-- 主键：多维度组合 + dt
-- 分区：按dt日期分区
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.dws_account_registry_source_di ON CLUSTER '{cluster}'
(
    -- 来源维度
    sex                         String          COMMENT '性别：男/女/保密',
    `level`                     Int32           COMMENT '等级(0-6)',
    age                         Int32           COMMENT '年龄',
    birth_year                  Int32           COMMENT '出生年份',
    vip_type                    Int32           COMMENT 'VIP类型：0=无,1=月度,2=年度',
    vip_type_name               String          COMMENT 'VIP类型名称',
    `status`                    Int32           COMMENT '账号状态：0=正常,1=封禁,2=注销',
    status_name                 String          COMMENT '账号状态名称',
    official_type               Int32           COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    is_official                 UInt8           COMMENT '是否认证用户',
    theme                       String          COMMENT '主题设置',
    primary_tag                 String          COMMENT '主要标签',

    -- 聚合指标
    new_account_cnt             Int64           COMMENT '新注册账号数',

    -- ETL信息
    dw_create_time              DateTime64(3)   COMMENT '数仓创建时间',
    dt                          String          COMMENT '日期分区'
)
ENGINE = ReplacingMergeTree(dw_create_time)
PARTITION BY dt
ORDER BY (dt, sex, `level`, age, birth_year, vip_type, `status`, official_type, theme, primary_tag)
SETTINGS index_granularity = 8192;

-- ============================================================
-- 4. dws_vip_order_source_di - VIP订单来源分析表
-- 主键：多维度组合 + dt
-- 分区：按dt日期分区
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.dws_vip_order_source_di ON CLUSTER '{cluster}'
(
    -- 订单维度
    order_status                String          COMMENT '订单状态',
    plan_id                     String          COMMENT '套餐ID',
    plan_name                   String          COMMENT '套餐名称',
    plan_duration_days          Int32           COMMENT '套餐时长(天)',
    pay_method                  String          COMMENT '支付方式',
    platform                    String          COMMENT '平台',
    channel                     String          COMMENT '渠道',
    from_page                   String          COMMENT '来源页面',
    current_vip_type            Int32           COMMENT '当前VIP类型',
    coupon_id                   String          COMMENT '优惠券ID',
    error_code                  String          COMMENT '错误码',
    cancel_stage                String          COMMENT '取消阶段',

    -- 用户维度
    sex                         String          COMMENT '性别：男/女/保密',
    age                         Int32           COMMENT '年龄',
    birth_year                  Int32           COMMENT '出生年份',
    `level`                     Int32           COMMENT '等级(0-6)',
    official_type               Int32           COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    is_official                 UInt8           COMMENT '是否认证用户',
    primary_tag                 String          COMMENT '主要标签',
    account_age_days            Int32           COMMENT '账号年龄(天)',
    follower_cnt                Int32           COMMENT '粉丝数',

    -- 聚合指标
    order_cnt                   Int64           COMMENT '订单数',
    order_user_cnt              Int64           COMMENT '下单用户数',
    original_price              Decimal(12,2)   COMMENT '原价',
    final_price                 Decimal(12,2)   COMMENT '实付价',
    discount_amount             Decimal(12,2)   COMMENT '优惠金额',
    pay_duration_sec            Int32           COMMENT '支付耗时(秒)',

    -- ETL信息
    dw_create_time              DateTime64(3)   COMMENT '数仓创建时间',
    dt                          String          COMMENT '日期分区'
)
ENGINE = ReplacingMergeTree(dw_create_time)
PARTITION BY dt
ORDER BY (dt, order_status, plan_id, pay_method, platform, channel, sex, `level`, official_type)
SETTINGS index_granularity = 8192;
