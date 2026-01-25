WITH
-- ============================================================
-- 1. 获取当日VIP订单数据
-- ============================================================
vip_orders AS (
    SELECT
        order_no,
        mid,
        order_status,
        plan_id,
        plan_name,
        plan_duration_days,
        original_price,
        final_price,
        discount_amount,
        coupon_id,
        pay_method,
        pay_duration_sec,
        current_vip_type,
        platform,
        channel,
        from_page,
        error_code,
        cancel_stage
    FROM dwd.dwd_order_vip_di
    WHERE dt = '${bizdate}'
      AND order_no IS NOT NULL
),

-- ============================================================
-- 2. 获取用户维度数据
-- ============================================================
user_dim AS (
    SELECT
        mid,
        sex,
        age,
        birth_year,
        level,
        official_type,
        is_official,
        primary_tag,
        account_age_days,
        follower_cnt
    FROM dim.dim_account_df
    WHERE dt = '${bizdate}'
),

-- ============================================================
-- 3. 订单关联用户维度
-- ============================================================
order_with_user AS (
    SELECT
        o.*,
        u.sex,
        u.age,
        u.birth_year,
        u.level,
        u.official_type,
        u.is_official,
        u.primary_tag,
        u.account_age_days,
        u.follower_cnt
    FROM vip_orders o
    LEFT JOIN user_dim u ON o.mid = u.mid
)

INSERT OVERWRITE TABLE dws.dws_vip_order_source_di PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 订单维度 ==========
    order_status,
    plan_id,
    plan_name,
    plan_duration_days,
    pay_method,
    platform,
    channel,
    from_page,
    current_vip_type,
    coupon_id,
    error_code,
    cancel_stage,

    -- ========== 用户维度 ==========
    sex,
    age,
    birth_year,
    level,
    official_type,
    is_official,
    primary_tag,
    account_age_days,
    follower_cnt,

    -- ========== 聚合指标 ==========
    -- 理论上为1，当作质量校验字段加入
    COUNT(1)                            AS order_cnt,
    COUNT(mid)                          AS order_user_cnt,

    MAX(COALESCE(original_price, 0))    AS original_price,
    MAX(COALESCE(final_price, 0))       AS final_price,
    MAX(COALESCE(discount_amount, 0))   AS discount_amount,
    MAX(COALESCE(pay_duration_sec, 0))  AS pay_duration_sec,

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP()             AS dw_create_time

FROM order_with_user
GROUP BY
    order_status,
    plan_id,
    plan_name,
    plan_duration_days,
    pay_method,
    platform,
    channel,
    from_page,
    current_vip_type,
    coupon_id,
    error_code,
    cancel_stage,
    sex,
    age,
    birth_year,
    level,
    official_type,
    is_official,
    primary_tag,
    account_age_days,
    follower_cnt;
