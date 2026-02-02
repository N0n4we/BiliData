CREATE TABLE IF NOT EXISTS dwd.dwd_order_vip_di (
    -- 主键
    order_no                STRING      COMMENT '订单号',

    -- 用户维度
    mid                     BIGINT      COMMENT '用户ID',
    current_vip_type        INT         COMMENT '下单时VIP类型: 0=无, 1=月度, 2=年度',
    current_vip_expire_ts   BIGINT      COMMENT '下单时VIP到期时间戳(ms)',

    -- 套餐维度
    plan_id                 STRING      COMMENT '套餐ID',
    plan_name               STRING      COMMENT '套餐名称',
    plan_duration_days      INT         COMMENT '套餐时长(天)',

    -- 金额
    original_price          BIGINT      COMMENT '原价(分)',
    final_price             BIGINT      COMMENT '实付金额(分)',
    discount_amount         BIGINT      COMMENT '优惠金额(分)',
    coupon_id               STRING      COMMENT '优惠券ID',

    -- 支付信息
    pay_method              STRING      COMMENT '支付方式',
    pay_duration_sec        INT         COMMENT '支付耗时(秒)',
    new_vip_expire_ts       BIGINT      COMMENT '支付成功后VIP到期时间戳(ms)',

    -- 订单状态
    order_status            STRING      COMMENT '订单状态: created/paying/success/fail/cancel',

    -- 里程碑时间戳
    create_ts               BIGINT      COMMENT '创建订单时间戳(ms)',
    pay_start_ts            BIGINT      COMMENT '开始支付时间戳(ms)',
    pay_end_ts              BIGINT      COMMENT '支付结束时间戳(ms)',

    -- 异常信息
    error_code              STRING      COMMENT '失败错误码',
    error_msg               STRING      COMMENT '失败错误信息',
    retry_count             INT         COMMENT '重试次数',
    cancel_stage            STRING      COMMENT '取消阶段',

    -- 设备环境
    device_id               STRING      COMMENT '设备ID',
    platform                STRING      COMMENT '平台',
    app_version             STRING      COMMENT 'APP版本',
    channel                 STRING      COMMENT '渠道',
    ip                      STRING      COMMENT 'IP',

    -- 追踪
    from_page               STRING      COMMENT '来源页面',
    trace_id                STRING      COMMENT '链路追踪ID',
    session_id              STRING      COMMENT '会话ID'
)
COMMENT 'DWD-VIP订单累计快照事实表'
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET;
