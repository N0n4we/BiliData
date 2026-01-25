-- 每日新注册账号来源分析表
-- 从dim_account_df获取今日mid，去除昨日mid得到新建账号，按多维度聚合
WITH
-- ============================================================
-- 1. 今日所有账号
-- ============================================================
today_accounts AS (
    SELECT
        mid,
        sex,
        level,
        age,
        birth_year,
        vip_type,
        vip_type_name,
        status,
        status_name,
        official_type,
        is_official,
        theme,
        primary_tag
    FROM dim.dim_account_df
    WHERE dt = '${bizdate}'
),

-- ============================================================
-- 2. 昨日所有账号mid
-- ============================================================
yesterday_mids AS (
    SELECT mid
    FROM dim.dim_account_df
    WHERE dt = DATE_SUB('${bizdate}', 1)
),

-- ============================================================
-- 3. 新注册账号（今日有、昨日无）
-- ============================================================
new_accounts AS (
    SELECT t.*
    FROM today_accounts t
    LEFT JOIN yesterday_mids y ON t.mid = y.mid
    WHERE y.mid IS NULL
)

INSERT OVERWRITE TABLE dws.dws_account_registry_source_di PARTITION (dt = '${bizdate}')
SELECT
    -- ========== 来源维度 ==========
    sex,
    level,
    age,
    birth_year,
    vip_type,
    vip_type_name,
    status,
    status_name,
    official_type,
    is_official,
    theme,
    primary_tag,

    -- ========== 聚合指标 ==========
    COUNT(*)                AS new_account_cnt,

    -- ========== ETL信息 ==========
    CURRENT_TIMESTAMP()     AS dw_create_time

FROM new_accounts
GROUP BY
    sex,
    level,
    age,
    birth_year,
    vip_type,
    vip_type_name,
    status,
    status_name,
    official_type,
    is_official,
    theme,
    primary_tag
