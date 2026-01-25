USE CATALOG hive_prod;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

CREATE TABLE IF NOT EXISTS dws.dws_account_registry_source_di (
    -- ========== 来源维度 ==========
    sex                     STRING          COMMENT '性别：男/女/保密',
    level                   INT             COMMENT '等级(0-6)',
    age                     INT             COMMENT '年龄',
    birth_year              INT             COMMENT '出生年份',
    vip_type                INT             COMMENT 'VIP类型：0=无,1=月度,2=年度',
    vip_type_name           STRING          COMMENT 'VIP类型名称',
    status                  INT             COMMENT '账号状态：0=正常,1=封禁,2=注销',
    status_name             STRING          COMMENT '账号状态名称',
    official_type           INT             COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    is_official             BOOLEAN         COMMENT '是否认证用户',
    theme                   STRING          COMMENT '主题设置',
    primary_tag             STRING          COMMENT '主要标签',

    -- ========== 聚合指标 ==========
    new_account_cnt         BIGINT          COMMENT '新注册账号数',

    -- ========== ETL信息 ==========
    dw_create_time          TIMESTAMP       COMMENT '数仓创建时间'
)
COMMENT '每日新注册账号来源分析表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
