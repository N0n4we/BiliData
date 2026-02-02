CREATE TABLE IF NOT EXISTS dim.dim_account_df (
    -- ========== 用户基础信息 ==========
    mid                     BIGINT          COMMENT '用户ID',
    nick_name               STRING          COMMENT '昵称',
    sex                     STRING          COMMENT '性别：男/女/保密',
    face_url                STRING          COMMENT '头像URL',
    sign                    STRING          COMMENT '个性签名',
    level                   INT             COMMENT '等级(0-6)',
    birthday                STRING          COMMENT '生日(YYYY-MM-DD)',
    age                     INT             COMMENT '年龄',
    birth_year              INT             COMMENT '出生年份',

    -- ========== 账号状态 ==========
    coins                   DECIMAL(12,2)   COMMENT '硬币数',
    vip_type                INT             COMMENT 'VIP类型：0=无,1=月度,2=年度',
    vip_type_name           STRING          COMMENT 'VIP类型名称',
    vip_expire              TIMESTAMP       COMMENT '会员截止时间',
    status                  INT             COMMENT '账号状态：0=正常,1=封禁,2=注销',
    status_name             STRING          COMMENT '账号状态名称',

    -- ========== 认证信息 ==========
    official_type           INT             COMMENT '认证类型：-1=无认证,0=个人认证,1=机构认证',
    official_desc           STRING          COMMENT '认证描述',
    is_official             BOOLEAN         COMMENT '是否认证用户',

    -- ========== 用户设置 ==========
    privacy_show_fav        BOOLEAN         COMMENT '是否公开收藏',
    privacy_show_history    BOOLEAN         COMMENT '是否公开历史',
    push_comment            BOOLEAN         COMMENT '是否开启评论推送',
    push_like               BOOLEAN         COMMENT '是否开启点赞推送',
    push_at                 BOOLEAN         COMMENT '是否开启@推送',
    theme                   STRING          COMMENT '主题设置',

    -- ========== 用户标签 ==========
    tags                    ARRAY<STRING>   COMMENT '用户标签列表',
    tag_cnt                 INT             COMMENT '标签数量',
    primary_tag             STRING          COMMENT '主要标签（首个）',

    -- ========== 账号时间信息 ==========
    created_at              TIMESTAMP       COMMENT '注册时间',
    updated_at              TIMESTAMP       COMMENT '最后更新时间',
    account_age_days        INT             COMMENT '账号年龄(天)',

    -- ========== 累计关注关系（当前状态）==========
    following_list          ARRAY<BIGINT>   COMMENT '当前关注的用户ID列表',
    following_cnt           INT             COMMENT '当前关注数',
    follower_list           ARRAY<BIGINT>   COMMENT '当前粉丝用户ID列表',
    follower_cnt            INT             COMMENT '当前粉丝数',
    mutual_follow_list      ARRAY<BIGINT>   COMMENT '当前互关用户ID列表',
    mutual_follow_cnt       INT             COMMENT '当前互关数',

    -- ========== 累计拉黑关系（当前状态）==========
    blocking_list           ARRAY<BIGINT>   COMMENT '当前拉黑的用户ID列表',
    blocking_cnt            INT             COMMENT '当前拉黑数',
    blocked_by_list         ARRAY<BIGINT>   COMMENT '被拉黑来源用户ID列表',
    blocked_by_cnt          INT             COMMENT '被拉黑数',

    -- ========== 关系变化趋势 ==========
    following_chg           INT             COMMENT '今日关注数变化（正为增加，负为减少）',
    follower_chg            INT             COMMENT '今日粉丝数变化',

    -- ========== ETL信息 ==========
    dw_create_time          TIMESTAMP       COMMENT '数仓创建时间'
)
COMMENT '账号维度表（每日全量快照，含累计关系）'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
