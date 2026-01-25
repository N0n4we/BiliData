USE CATALOG hive_prod;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

CREATE TABLE IF NOT EXISTS dws.dws_vip_order_source_di (
    -- ========== 订单维度 ==========
    order_status            STRING          COMMENT '订单状态',
    plan_id                 STRING          COMMENT '套餐ID',
    plan_name               STRING          COMMENT '套餐名称',
    plan_duration_days      INT             COMMENT '套餐时长(天)',
    pay_method              STRING          COMMENT '支付方式',
    platform                STRING          COMMENT '平台',
    channel                 STRING          COMMENT '渠道',
    from_page               STRING          COMMENT '来源页面',
    current_vip_type        INT             COMMENT '当前VIP类型',
    coupon_id               STRING          COMMENT '优惠券ID',
    error_code              STRING          COMMENT '错误码',
    cancel_stage            STRING          COMMENT '取消阶段',

    -- ========== 用户维度 ==========
    sex                     STRING          COMMENT '性别：男/女/保密',
    age                     INT             COMMENT '年龄',
    birth_year              INT             COMMENT '出生年份',
    level                   INT             COMMENT '等级(0-6)',
    official_type           INT             COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    is_official             BOOLEAN         COMMENT '是否认证用户',
    primary_tag             STRING          COMMENT '主要标签',
    account_age_days        INT             COMMENT '账号年龄(天)',
    follower_cnt            INT             COMMENT '粉丝数',

    -- ========== 聚合指标 ==========
    order_cnt               BIGINT          COMMENT '订单数',
    order_user_cnt          BIGINT          COMMENT '下单用户数',
    original_price          DECIMAL(12,2)   COMMENT '原价',
    final_price             DECIMAL(12,2)   COMMENT '实付价',
    discount_amount         DECIMAL(12,2)   COMMENT '优惠金额',
    pay_duration_sec        INT             COMMENT '支付耗时(秒)',

    -- ========== ETL信息 ==========
    dw_create_time          TIMESTAMP       COMMENT '数仓创建时间'
)
COMMENT 'VIP订单来源分析表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
