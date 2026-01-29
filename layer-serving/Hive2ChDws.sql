-- ============================================================
-- Hive2ChDws - 批处理层将Hive dws表同步到ClickHouse
-- 功能：将Hive中dt=${bizdate}的所有dws_*表overwrite到ClickHouse
-- 模式：按bizdate分区覆盖写入
--
-- 包含表：
--   1. dws_video_stats_account_di -> dws_video_stats_account_di
--   2. dws_account_registry_source_di -> dws_account_registry_source_di
--   3. dws_vip_order_source_di -> dws_vip_order_source_di
-- ============================================================

SET 'execution.runtime-mode' = 'batch';

-- ============================================================
-- 1. 创建 Hive Catalog
-- ============================================================
CREATE CATALOG hive_prod WITH (
    'type' = 'hive',
    'hive-conf-dir' = '/opt/hive/conf'
);

USE CATALOG hive_prod;

-- ============================================================
-- 2. 创建 ClickHouse Sink 表 - dws_video_stats_account_di
-- ============================================================
DROP TABLE IF EXISTS clickhouse_dws_video_stats_account;
CREATE TABLE clickhouse_dws_video_stats_account (
    mid                         BIGINT,
    nick_name                   STRING,
    sex                         STRING,
    `level`                     INT,
    vip_type                    INT,
    vip_type_name               STRING,
    official_type               INT,
    official_desc               STRING,
    is_official                 BOOLEAN,
    follower_cnt                INT,
    following_cnt               INT,
    video_cnt                   BIGINT,
    total_view_count            BIGINT,
    total_view_user_count       BIGINT,
    total_play_duration_sec     BIGINT,
    avg_play_duration_sec       INT,
    total_like_delta            BIGINT,
    total_unlike_delta          BIGINT,
    total_net_like_delta        BIGINT,
    total_coin_count_delta      BIGINT,
    total_coin_total_delta      BIGINT,
    total_favorite_delta        BIGINT,
    total_unfavorite_delta      BIGINT,
    total_net_favorite_delta    BIGINT,
    total_share_count           BIGINT,
    total_danmaku_count         BIGINT,
    total_triple_count          BIGINT,
    dw_create_time              TIMESTAMP(3),
    dt                          STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_video_stats_account_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

-- ============================================================
-- 3. 创建 ClickHouse Sink 表 - dws_account_registry_source_di
-- ============================================================
DROP TABLE IF EXISTS clickhouse_dws_account_registry_source;
CREATE TABLE clickhouse_dws_account_registry_source (
    sex                     STRING,
    `level`                 INT,
    age                     INT,
    birth_year              INT,
    vip_type                INT,
    vip_type_name           STRING,
    `status`                INT,
    status_name             STRING,
    official_type           INT,
    is_official             BOOLEAN,
    theme                   STRING,
    primary_tag             STRING,
    new_account_cnt         BIGINT,
    dw_create_time          TIMESTAMP(3),
    dt                      STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_account_registry_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

-- ============================================================
-- 4. 创建 ClickHouse Sink 表 - dws_vip_order_source_di
-- ============================================================
DROP TABLE IF EXISTS clickhouse_dws_vip_order_source;
CREATE TABLE clickhouse_dws_vip_order_source (
    order_status            STRING,
    plan_id                 STRING,
    plan_name               STRING,
    plan_duration_days      INT,
    pay_method              STRING,
    platform                STRING,
    channel                 STRING,
    from_page               STRING,
    current_vip_type        INT,
    coupon_id               STRING,
    error_code              STRING,
    cancel_stage            STRING,
    sex                     STRING,
    age                     INT,
    birth_year              INT,
    `level`                 INT,
    official_type           INT,
    is_official             BOOLEAN,
    primary_tag             STRING,
    account_age_days        INT,
    follower_cnt            INT,
    order_cnt               BIGINT,
    order_user_cnt          BIGINT,
    original_price          DECIMAL(12,2),
    final_price             DECIMAL(12,2),
    discount_amount         DECIMAL(12,2),
    pay_duration_sec        INT,
    dw_create_time          TIMESTAMP(3),
    dt                      STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/dws?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_vip_order_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '15s'
);

-- ============================================================
-- 5. 同步 dws_video_stats_account_di 到 ClickHouse
-- 主键: mid, dt
-- ============================================================
INSERT INTO clickhouse_dws_video_stats_account
SELECT
    COALESCE(mid, 0) AS mid, -- 主键防空
    nick_name,
    sex,
    `level`,
    vip_type,
    vip_type_name,
    official_type,
    official_desc,
    is_official,
    follower_cnt,
    following_cnt,
    video_cnt,
    total_view_count,
    total_view_user_count,
    total_play_duration_sec,
    avg_play_duration_sec,
    total_like_delta,
    total_unlike_delta,
    total_net_like_delta,
    total_coin_count_delta,
    total_coin_total_delta,
    total_favorite_delta,
    total_unfavorite_delta,
    total_net_favorite_delta,
    total_share_count,
    total_danmaku_count,
    total_triple_count,
    dw_create_time,
    dt
FROM dws.dws_video_stats_account_di
WHERE dt = DATE_FORMAT(CAST(CURRENT_DATE - INTERVAL '1' DAY AS TIMESTAMP), 'yyyy-MM-dd');

-- ============================================================
-- 6. 同步 dws_account_registry_source_di 到 ClickHouse
-- 主键: sex, level, age, birth_year, vip_type, status, official_type, theme, primary_tag, dt
-- ============================================================
INSERT INTO clickhouse_dws_account_registry_source
SELECT
    COALESCE(sex, 'unknown') AS sex, -- 主键防空，String默认为unknown或空串
    COALESCE(`level`, -1) AS `level`, -- 主键防空，Int默认为-1或0
    COALESCE(age, -1) AS age,        -- 主键防空
    COALESCE(birth_year, 0) AS birth_year, -- 主键防空
    COALESCE(vip_type, 0) AS vip_type,     -- 主键防空
    vip_type_name,
    COALESCE(`status`, 0) AS `status`,     -- 主键防空
    status_name,
    COALESCE(official_type, -1) AS official_type, -- 主键防空
    is_official,
    COALESCE(theme, '') AS theme,          -- 主键防空
    COALESCE(primary_tag, '') AS primary_tag, -- 主键防空 (之前的报错点)
    new_account_cnt,
    dw_create_time,
    dt
FROM dws.dws_account_registry_source_di
WHERE dt = DATE_FORMAT(CAST(CURRENT_DATE - INTERVAL '1' DAY AS TIMESTAMP), 'yyyy-MM-dd');

-- ============================================================
-- 7. 同步 dws_vip_order_source_di 到 ClickHouse
-- 主键: order_status, plan_id, pay_method, platform, channel, sex, level, official_type, dt
-- ============================================================
INSERT INTO clickhouse_dws_vip_order_source
SELECT
    COALESCE(order_status, '') AS order_status, -- 主键防空
    COALESCE(plan_id, '') AS plan_id,           -- 主键防空 (本次的报错点)
    plan_name,
    plan_duration_days,
    COALESCE(pay_method, '') AS pay_method,     -- 主键防空
    COALESCE(platform, '') AS platform,         -- 主键防空
    COALESCE(channel, '') AS channel,           -- 主键防空
    from_page,
    current_vip_type,
    coupon_id,
    error_code,
    cancel_stage,
    COALESCE(sex, 'unknown') AS sex,            -- 主键防空
    age, -- age 不在表3的主键中，可以不处理，但为了稳健也可处理
    birth_year, -- 同上
    COALESCE(`level`, -1) AS `level`,           -- 主键防空
    COALESCE(official_type, -1) AS official_type, -- 主键防空
    is_official,
    primary_tag,
    account_age_days,
    follower_cnt,
    order_cnt,
    order_user_cnt,
    original_price,
    final_price,
    discount_amount,
    pay_duration_sec,
    dw_create_time,
    dt
FROM dws.dws_vip_order_source_di
WHERE dt = DATE_FORMAT(CAST(CURRENT_DATE - INTERVAL '1' DAY AS TIMESTAMP), 'yyyy-MM-dd');
