-- 切换到 dwd 库
USE CATALOG hive_prod;
CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

-- 创建视频行为事件明细表（每日增量）
CREATE TABLE IF NOT EXISTS dwd.dwd_action_video_di (
    -- ========== 数据质量校验 ==========
    is_valid            BOOLEAN     COMMENT '数据是否有效',
    invalid_reason      STRING      COMMENT '无效原因',

    -- ========== 事件标识 ==========
    event_id            STRING      COMMENT '事件类型标识',
    trace_id            STRING      COMMENT '链路追踪ID',
    session_id          STRING      COMMENT '会话ID',

    -- ========== 用户与对象 ==========
    mid                 BIGINT      COMMENT '用户ID',
    bvid                STRING      COMMENT '视频BV号',

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

    -- ========== 观看属性 ==========
    view_from           STRING      COMMENT '观看来源(recommend/search/space等)',
    spm_id              STRING      COMMENT 'SPM埋点ID',
    is_auto_play        BOOLEAN     COMMENT '是否自动播放',
    quality             STRING      COMMENT '画质',

    -- ========== 播放心跳属性 ==========
    progress_sec        INT         COMMENT '播放进度(秒)',
    total_sec           INT         COMMENT '视频总时长(秒)',
    play_speed          DOUBLE      COMMENT '播放速度',
    is_full_screen      BOOLEAN     COMMENT '是否全屏',

    -- ========== 上传属性 ==========
    upload_id           STRING      COMMENT '上传任务ID',
    file_size_mb        INT         COMMENT '文件大小(MB)',
    duration_sec        INT         COMMENT '视频时长(秒)',
    resolution          STRING      COMMENT '分辨率',
    upload_type         STRING      COMMENT '上传类型',
    progress_percent    INT         COMMENT '上传进度百分比',
    uploaded_mb         INT         COMMENT '已上传大小(MB)',
    speed_kbps          INT         COMMENT '上传速度(kbps)',
    upload_duration_sec INT         COMMENT '上传耗时(秒)',
    title               STRING      COMMENT '视频标题',
    category            STRING      COMMENT '视频分区',

    -- ========== 上传失败属性 ==========
    error_code          STRING      COMMENT '错误码',
    error_msg           STRING      COMMENT '错误信息',
    retry_count         INT         COMMENT '重试次数',

    -- ========== 修改/删除属性 ==========
    updated_fields      STRING      COMMENT '修改的字段(JSON数组)',
    delete_reason       STRING      COMMENT '删除原因',
    video_age_days      INT         COMMENT '视频存在天数',

    -- ========== 互动属性 ==========
    action_from         STRING      COMMENT '互动来源页面',
    is_liked_before     BOOLEAN     COMMENT '之前是否已点赞',
    coin_count          INT         COMMENT '投币数量',
    with_like           BOOLEAN     COMMENT '投币时是否点赞',
    remain_coins        INT         COMMENT '剩余硬币数',
    fav_folder_ids      STRING      COMMENT '收藏夹ID列表(JSON数组)',
    create_new_folder   BOOLEAN     COMMENT '是否创建新收藏夹',
    share_channel       STRING      COMMENT '分享渠道',
    share_result        STRING      COMMENT '分享结果',

    -- ========== 弹幕属性 ==========
    danmaku_content     STRING      COMMENT '弹幕内容',
    danmaku_progress_sec INT        COMMENT '弹幕发送时视频进度(秒)',
    danmaku_type        STRING      COMMENT '弹幕类型(scroll/top/bottom)',
    danmaku_color       STRING      COMMENT '弹幕颜色',

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
