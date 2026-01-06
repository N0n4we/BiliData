# Mock 数据生成器说明

本脚本通过 Kafka 生成模拟的 B 站业务数据和埋点数据。

## 业务表

| 表名              | 说明   | 默认数量 |
|-------------------|--------|----------|
| app_user_profile  | 用户表 | 200      |
| app_video_content | 视频表 | 100      |
| app_comment       | 评论表 | 300      |

### app_user_profile（用户表）

| 字段            | 类型     | 说明                                          |
|-----------------|----------|-----------------------------------------------|
| mid             | int      | 用户ID（8位数字）                             |
| nick_name       | string   | 昵称                                          |
| sex             | string   | 性别：男/女/保密                              |
| face_url        | string   | 头像URL                                       |
| sign            | string   | 个性签名                                      |
| level           | int      | 等级（0-6）                                   |
| birthday        | string   | 生日（YYYY-MM-DD）                            |
| coins           | float    | 硬币数                                        |
| vip_type        | int      | VIP类型：0=无，1=月度，2=年度                 |
| official_verify | object   | 认证信息：{type, desc}                        |
| settings        | object   | 设置：{privacy, push, theme}                  |
| tags            | array    | 用户标签                                      |
| status          | int      | 状态                                          |
| created_at      | long     | 创建时间戳(ms)                                |
| updated_at      | long     | 更新时间戳(ms)                                |

### app_video_content（视频表）

| 字段          | 类型   | 说明                                                 |
|---------------|--------|------------------------------------------------------|
| bvid          | string | 视频BV号                                             |
| title         | string | 标题                                                 |
| cover         | string | 封面URL                                              |
| desc_text     | string | 简介                                                 |
| duration      | int    | 时长（秒）                                           |
| pubdate       | long   | 发布时间戳(ms)                                       |
| mid           | int    | UP主ID                                               |
| category_id   | int    | 分区ID                                               |
| category_name | string | 分区名                                               |
| state         | int    | 状态                                                 |
| attribute     | int    | 属性                                                 |
| is_private    | bool   | 是否私密                                             |
| meta_info     | object | 元信息：{upload_ip, camera, software, resolution, fps} |
| stats         | object | 统计：{view, danmaku, reply, favorite, coin, share, like} |
| audit_info    | array  | 审核记录                                             |
| created_at    | long   | 创建时间戳(ms)                                       |
| updated_at    | long   | 更新时间戳(ms)                                       |

### app_comment（评论表）

| 字段          | 类型   | 说明                     |
|---------------|--------|--------------------------|
| rpid          | int    | 评论ID                   |
| oid           | string | 视频BV号                 |
| otype         | int    | 对象类型（1=视频）       |
| mid           | int    | 发表者ID                 |
| root          | int    | 根评论ID（0=根评论）     |
| parent        | int    | 父评论ID                 |
| content       | string | 评论内容                 |
| like_count    | int    | 点赞数                   |
| dislike_count | int    | 点踩数                   |
| reply_count   | int    | 回复数                   |
| state         | int    | 状态                     |
| created_at    | long   | 创建时间戳(ms)           |
| updated_at    | long   | 更新时间戳(ms)           |

---

## 埋点表

| 表名              | 说明         | 默认数量 |
|-------------------|--------------|----------|
| app_event_account | 账号埋点     | 500      |
| app_event_video   | 视频埋点     | 2000     |
| app_event_social  | 社交互动埋点 | 800      |
| app_event_comment | 评论埋点     | 1000     |
| app_event_vip     | VIP购买埋点  | 400      |

### 埋点通用字段

所有埋点表共享以下字段：

| 字段        | 类型   | 说明                     |
|-------------|--------|--------------------------|
| event_id    | string | 事件类型标识             |
| trace_id    | string | 链路追踪ID（32位hex）    |
| session_id  | string | 会话ID（UUID格式）       |
| mid         | int    | 用户ID                   |
| client_ts   | long   | 客户端时间戳(ms)         |
| server_ts   | long   | 服务端时间戳(ms)         |
| url_path    | string | 页面路径                 |
| referer     | string | 来源页面                 |
| ua          | string | User-Agent               |
| ip          | string | 客户端IP                 |
| device_info | object | 设备信息（见下）         |
| app_context | object | APP上下文（见下）        |
| properties  | object | 事件属性（因事件而异）   |

### device_info 结构

| 字段          | 类型   | 说明                           |
|---------------|--------|--------------------------------|
| device_id     | string | 设备唯一ID                     |
| idfa          | string | iOS广告标识符                  |
| oaid          | string | Android开放匿名标识符          |
| android_id    | string | Android设备ID                  |
| brand         | string | 品牌（Apple/Xiaomi/Huawei等）  |
| model         | string | 型号                           |
| os            | string | 操作系统（iOS/Android）        |
| os_version    | string | 系统版本                       |
| screen_width  | int    | 屏幕宽度                       |
| screen_height | int    | 屏幕高度                       |
| carrier       | string | 运营商                         |
| network       | string | 网络类型（WIFI/5G/4G等）       |
| battery_level | int    | 电池电量                       |
| is_charging   | bool   | 是否充电中                     |
| timezone      | string | 时区                           |
| lang          | string | 语言                           |
| dpi           | int    | 屏幕密度                       |

### app_context 结构

| 字段             | 类型   | 说明                           |
|------------------|--------|--------------------------------|
| app_version      | string | APP版本号                      |
| build_number     | int    | 构建号                         |
| channel          | string | 渠道来源                       |
| platform         | string | 平台（ios/android/ipad等）     |
| ab_test_groups   | array  | AB测试分组                     |
| push_enabled     | bool   | 是否开启推送                   |
| location_enabled | bool   | 是否开启定位                   |
| sdk_version      | string | SDK版本                        |

---

## 埋点事件类型

### 1. 账号埋点 (app_event_account)

| 事件类型                | 说明     | 特有属性                                       |
|-------------------------|----------|------------------------------------------------|
| account_register        | 注册     | register_type, invite_code, channel            |
| account_login           | 登录     | login_type, is_auto_login, login_result        |
| account_logout          | 登出     | -                                              |
| account_update_profile  | 修改资料 | updated_fields, before_snapshot, after_snapshot|
| account_update_avatar   | 修改头像 | avatar_source, file_size_kb                    |
| account_update_password | 修改密码 | -                                              |
| account_deactivate      | 注销账号 | reason, account_age_days                       |
| account_bind_phone      | 绑定手机 | bind_result, is_rebind                         |
| account_bind_email      | 绑定邮箱 | bind_result, is_rebind                         |

### 2. 视频埋点 (app_event_video)

附加字段：`bvid`（视频BV号）

| 事件类型             | 说明     | 特有属性                                              |
|----------------------|----------|-------------------------------------------------------|
| video_view           | 观看视频 | from, spm_id, is_auto_play, quality                   |
| video_play_heartbeat | 播放心跳 | progress_sec, total_sec, play_speed, quality          |
| video_upload_start   | 开始上传 | file_size_mb, duration_sec, resolution                |
| video_upload_progress| 上传进度 | upload_id, progress_percent, uploaded_mb, speed_kbps  |
| video_upload_complete| 上传完成 | title, category, duration_sec, upload_duration_sec    |
| video_upload_fail    | 上传失败 | error_code, error_msg, retry_count                    |
| video_update_info    | 修改信息 | updated_fields                                        |
| video_delete         | 删除视频 | delete_reason, video_age_days                         |
| video_set_private    | 设为私密 | -                                                     |
| video_publish        | 发布视频 | -                                                     |
| video_like           | 点赞     | from, is_liked_before                                 |
| video_unlike         | 取消点赞 | from, is_liked_before                                 |
| video_coin           | 投币     | coin_count, with_like, remain_coins                   |
| video_favorite       | 收藏     | fav_folder_ids, create_new_folder                     |
| video_unfavorite     | 取消收藏 | fav_folder_ids                                        |
| video_share          | 分享     | share_channel, share_result                           |
| video_danmaku        | 发弹幕   | content, progress_sec, danmaku_type, color            |
| video_triple         | 一键三连 | from, coin_count                                      |

### 3. 社交埋点 (app_event_social)

附加字段：`target_mid`（目标用户ID）

| 事件类型           | 说明     | 特有属性                                           |
|--------------------|----------|----------------------------------------------------|
| social_follow      | 关注     | target_level, target_follower_count, is_mutual     |
| social_unfollow    | 取消关注 | follow_duration_days, unfollow_reason              |
| social_block       | 拉黑     | block_reason, is_following                         |
| social_unblock     | 取消拉黑 | -                                                  |
| social_whisper     | 私信     | msg_type, msg_length, is_first_msg                 |
| social_report_user | 举报用户 | report_reason, report_evidence                     |

### 4. 评论埋点 (app_event_comment)

附加字段：`oid`（视频BV号）, `rpid`（评论ID）

| 事件类型         | 说明       | 特有属性                                         |
|------------------|------------|--------------------------------------------------|
| comment_create   | 发表评论   | content, content_length, has_emoji, has_at       |
| comment_reply    | 回复评论   | root_rpid, parent_rpid, reply_to_mid             |
| comment_update   | 修改评论   | old_content, new_content, edit_reason            |
| comment_delete   | 删除评论   | delete_reason, comment_age_hours                 |
| comment_report   | 举报评论   | report_reason, report_content_preview            |
| comment_like     | 评论点赞   | is_root_comment, comment_owner_mid               |
| comment_unlike   | 取消点赞   | is_root_comment, comment_owner_mid               |
| comment_dislike  | 评论点踩   | is_root_comment                                  |
| comment_undislike| 取消点踩   | is_root_comment                                  |

### 5. VIP埋点 (app_event_vip)

附加字段：`order_no`（订单号，部分事件）

| 事件类型          | 说明       | 特有属性                                          |
|-------------------|------------|---------------------------------------------------|
| vip_page_view     | 浏览VIP页  | from, page_load_ms, is_promotion_period           |
| vip_select_plan   | 选择套餐   | plan_id, plan_name, plan_price, plan_duration_days|
| vip_create_order  | 创建订单   | original_price, final_price, coupon_id            |
| vip_pay_start     | 开始支付   | pay_method, amount                                |
| vip_pay_success   | 支付成功   | pay_method, amount, pay_duration_sec, new_vip_expire |
| vip_pay_fail      | 支付失败   | error_code, error_msg, retry_count                |
| vip_pay_cancel    | 取消支付   | cancel_stage, time_on_pay_page_sec                |
| vip_auto_renew_on | 开启自动续费| plan_id, from                                    |
| vip_auto_renew_off| 关闭自动续费| plan_id, from                                    |

---

## 数据特点

- 每个埋点包含完整的 `device_info`（设备信息）和 `app_context`（APP上下文）
- `properties` 字段根据事件类型包含不同的业务属性
- 支持 `trace_id`、`session_id` 用于链路追踪
- 默认生成数据量：200用户、100视频、300评论、4700+埋点

## 使用方式

```bash
# 启动 Kafka 后运行
python mock.py
```

生成的 Kafka Topics：
- `app_user_profile` - 用户数据
- `app_video_content` - 视频数据
- `app_comment` - 评论数据
- `app_event_account` - 账号埋点
- `app_event_video` - 视频埋点
- `app_event_social` - 社交埋点
- `app_event_comment` - 评论埋点
- `app_event_vip` - VIP埋点
