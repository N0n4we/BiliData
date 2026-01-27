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
    dt                          STRING,
    PRIMARY KEY (mid, dt) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/default?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_video_stats_account_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '30s'
);

-- ============================================================
-- 3. 创建 ClickHouse Sink 表 - dws_account_registry_source_di
-- ============================================================
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
    dt                      STRING,
    PRIMARY KEY (sex, `level`, age, birth_year, vip_type, `status`, official_type, theme, primary_tag, dt) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/default?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_account_registry_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '30s'
);

-- ============================================================
-- 4. 创建 ClickHouse Sink 表 - dws_vip_order_source_di
-- ============================================================
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
    dt                      STRING,
    PRIMARY KEY (order_status, plan_id, pay_method, platform, channel, sex, `level`, official_type, dt) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://clickhouse:9004/default?useSSL=false&allowPublicKeyRetrieval=true',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 'dws_vip_order_source_di',
    'username' = 'default',
    'password' = '',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval' = '30s'
);

-- ============================================================
-- 5. 同步 dws_video_stats_account_di 到 ClickHouse
-- ============================================================
INSERT INTO clickhouse_dws_video_stats_account
SELECT
    mid,
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
WHERE dt = '${bizdate}';

-- ============================================================
-- 6. 同步 dws_account_registry_source_di 到 ClickHouse
-- ============================================================
INSERT INTO clickhouse_dws_account_registry_source
SELECT
    sex,
    `level`,
    age,
    birth_year,
    vip_type,
    vip_type_name,
    `status`,
    status_name,
    official_type,
    is_official,
    theme,
    primary_tag,
    new_account_cnt,
    dw_create_time,
    dt
FROM dws.dws_account_registry_source_di
WHERE dt = '${bizdate}';

-- ============================================================
-- 7. 同步 dws_vip_order_source_di 到 ClickHouse
-- ============================================================
INSERT INTO clickhouse_dws_vip_order_source
SELECT
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
    `level`,
    official_type,
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
WHERE dt = '${bizdate}';
