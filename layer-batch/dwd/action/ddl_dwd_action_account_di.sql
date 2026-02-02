CREATE TABLE IF NOT EXISTS dwd.dwd_action_account_di (
    -- ========== 事件标识 ==========
    event_id            STRING      COMMENT '事件类型：social_follow/social_unfollow/social_block/social_unblock/social_whisper/social_report_user',
    trace_id            STRING      COMMENT '链路追踪ID',
    session_id          STRING      COMMENT '会话ID',

    -- ========== 用户信息 ==========
    mid                 BIGINT      COMMENT '行为主体用户ID',
    target_mid          BIGINT      COMMENT '行为对象用户ID',

    -- ========== 时间信息 ==========
    client_ts           BIGINT      COMMENT '客户端时间戳(ms)',
    server_ts           BIGINT      COMMENT '服务端时间戳(ms)',
    event_time          TIMESTAMP   COMMENT '事件时间（server_ts转换）',

    -- ========== 页面信息 ==========
    url_path            STRING      COMMENT '当前页面路径',
    referer             STRING      COMMENT '来源页面路径',

    -- ========== 设备信息 ==========
    device_id           STRING      COMMENT '设备唯一ID',
    brand               STRING      COMMENT '设备品牌',
    model               STRING      COMMENT '设备型号',
    os                  STRING      COMMENT '操作系统：Android/iOS',
    os_version          STRING      COMMENT '系统版本号',
    network             STRING      COMMENT '网络类型：wifi/4g/5g',
    ip                  STRING      COMMENT '客户端IP地址',

    -- ========== APP上下文 ==========
    app_version         STRING      COMMENT 'APP版本号',
    channel             STRING      COMMENT '渠道来源',
    platform            STRING      COMMENT '平台：android/ios/web',

    -- ========== 通用行为属性 ==========
    action_from         STRING      COMMENT '行为触发来源页面/入口',

    -- ========== 关注行为属性 ==========
    target_level        INT         COMMENT '目标用户等级',
    target_follower_cnt BIGINT      COMMENT '目标用户粉丝数',
    is_mutual           BOOLEAN     COMMENT '是否形成互关',
    special_group       STRING      COMMENT '特别关注分组名称',

    -- ========== 取关行为属性 ==========
    follow_duration_days INT        COMMENT '关注时长(天)',
    unfollow_reason     STRING      COMMENT '取关原因',

    -- ========== 拉黑行为属性 ==========
    block_reason        STRING      COMMENT '拉黑原因',
    is_following        BOOLEAN     COMMENT '拉黑前是否关注该用户',

    -- ========== 私信行为属性 ==========
    msg_type            STRING      COMMENT '消息类型：text/image/video/emoji',
    msg_length          INT         COMMENT '消息文本长度',
    is_first_msg        BOOLEAN     COMMENT '是否与该用户的首次私信',

    -- ========== 举报行为属性 ==========
    report_reason       STRING      COMMENT '举报原因分类',
    report_evidence     STRING      COMMENT '举报证据类型：screenshot/link/none'
)
COMMENT '账号社交行为明细表'
PARTITIONED BY (dt STRING COMMENT '日期分区，格式：yyyyMMdd')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
