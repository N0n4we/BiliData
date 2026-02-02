CREATE TABLE IF NOT EXISTS dwd.dwd_action_comment_di (
    -- ========== 事件标识 ==========
    event_id            STRING      COMMENT '事件类型标识',
    trace_id            STRING      COMMENT '链路追踪ID',
    session_id          STRING      COMMENT '会话ID',

    -- ========== 用户与对象 ==========
    mid                 BIGINT      COMMENT '用户ID',
    oid                 STRING      COMMENT '视频BV号',
    rpid                BIGINT      COMMENT '评论ID',
    otype               INT         COMMENT '对象类型：1=视频',

    -- ========== 时间信息 ==========
    client_ts           BIGINT      COMMENT '客户端时间戳(ms)',
    server_ts           BIGINT      COMMENT '服务端时间戳(ms)',
    event_time          TIMESTAMP   COMMENT '事件时间',
    event_date          STRING      COMMENT '事件日期',
    event_hour          STRING      COMMENT '事件小时',

    -- ========== 页面信息 ==========
    url_path            STRING      COMMENT '页面路径',
    referer             STRING      COMMENT '来源页面',
    ua                  STRING      COMMENT 'User-Agent',
    ip                  STRING      COMMENT '客户端IP',

    -- ========== 设备信息（展开） ==========
    device_id           STRING      COMMENT '设备唯一ID',
    brand               STRING      COMMENT '品牌',
    model               STRING      COMMENT '型号',
    os                  STRING      COMMENT '操作系统',
    os_version          STRING      COMMENT '系统版本',
    screen_width        INT         COMMENT '屏幕宽度',
    screen_height       INT         COMMENT '屏幕高度',
    carrier             STRING      COMMENT '运营商',
    network             STRING      COMMENT '网络类型',

    -- ========== APP上下文（展开） ==========
    app_version         STRING      COMMENT 'APP版本号',
    platform            STRING      COMMENT '平台',
    channel             STRING      COMMENT '渠道来源',

    -- ========== 评论创建属性 ==========
    content             STRING      COMMENT '评论内容',
    content_length      INT         COMMENT '内容长度',
    has_emoji           BOOLEAN     COMMENT '是否包含表情',
    has_at              BOOLEAN     COMMENT '是否包含@',
    from_page           STRING      COMMENT '来源页面',

    -- ========== 评论回复属性 ==========
    root_rpid           BIGINT      COMMENT '根评论ID',
    parent_rpid         BIGINT      COMMENT '父评论ID',
    reply_to_mid        BIGINT      COMMENT '回复目标用户ID',

    -- ========== 评论互动属性 ==========
    is_root_comment     BOOLEAN     COMMENT '是否根评论',
    comment_owner_mid   BIGINT      COMMENT '评论所有者ID',

    -- ========== 评论修改/删除属性 ==========
    old_content         STRING      COMMENT '修改前内容',
    new_content         STRING      COMMENT '修改后内容',
    edit_reason         STRING      COMMENT '修改原因',
    delete_reason       STRING      COMMENT '删除原因',
    comment_age_hours   INT         COMMENT '评论存在时长(小时)',

    -- ========== 举报属性 ==========
    report_reason       STRING      COMMENT '举报原因',
    report_content_preview STRING   COMMENT '举报内容预览',

    -- ========== 原始数据 ==========
    properties          STRING      COMMENT '事件属性JSON',
    device_info         STRING      COMMENT '设备信息JSON',
    app_context         STRING      COMMENT 'APP上下文JSON'
)
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
