# JSON 结构示例

本文档包含所有表的 JSON 结构示例。

---

## 业务表

### 1. app_user_profile（用户表）

```json
{
  "mid": 12345678,
  "nick_name": "王伟99",
  "sex": "男",
  "face_url": "https://i0.hdslb.com/bfs/face/a1b2c3d4e5f6.jpg",
  "sign": "这个人很懒",
  "level": 5,
  "birthday": "1995-06-15",
  "coins": 1234.5,
  "vip_type": 2,
  "official_verify": {
    "type": 0,
    "desc": "bilibili 知名UP主"
  },
  "settings": {
    "privacy": {
      "show_fav": true,
      "show_history": false
    },
    "push": {
      "comment": true,
      "like": true,
      "at": true
    },
    "theme": "dark"
  },
  "tags": ["宅", "技术宅", "游戏"],
  "status": 0,
  "created_at": 1705987200000,
  "updated_at": 1705987200000
}
```

### 2. app_video_content（视频表）

```json
{
  "bvid": "BV1abc12345",
  "title": "【原创】年度最佳_abc123",
  "cover": "https://i0.hdslb.com/bfs/archive/a1b2c3d4e5f6.jpg",
  "desc_text": "视频简介...",
  "duration": 360,
  "pubdate": 1705987200000,
  "mid": 12345678,
  "category_id": 17,
  "category_name": "game",
  "state": 0,
  "attribute": 0,
  "is_private": false,
  "meta_info": {
    "upload_ip": "192.168.1.100",
    "camera": "Sony A7M4",
    "software": "Premiere Pro",
    "resolution": "4k",
    "fps": 60
  },
  "stats": {
    "view": 100000,
    "danmaku": 5000,
    "reply": 2000,
    "favorite": 10000,
    "coin": 15000,
    "share": 3000,
    "like": 25000
  },
  "audit_info": [
    {
      "ts": 1705900800,
      "operator": "system",
      "status": "pass"
    }
  ],
  "created_at": 1705987200000,
  "updated_at": 1705987200000
}
```

### 3. app_comment（评论表）

```json
{
  "rpid": 123456789,
  "oid": "BV1abc12345",
  "otype": 1,
  "mid": 12345678,
  "root": 0,
  "parent": 0,
  "content": "太强了！",
  "like_count": 500,
  "dislike_count": 0,
  "reply_count": 20,
  "state": 0,
  "created_at": 1705987200000,
  "updated_at": 1705987200000
}
```

---

## 埋点表通用结构

所有埋点表共享以下基础结构：

```json
{
  "event_id": "事件类型标识",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/页面路径",
  "referer": "来源页面",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": { "..." }
}
```

### device_info 结构

```json
{
  "device_id": "550e8400-e29b-41d4-a716-446655440000",
  "idfa": "550e8400-e29b-41d4-a716-446655440001",
  "oaid": "",
  "android_id": "",
  "brand": "Apple",
  "model": "iPhone15,2",
  "os": "iOS",
  "os_version": "17.1",
  "screen_width": 1170,
  "screen_height": 2532,
  "carrier": "ChinaMobile",
  "network": "WIFI",
  "battery_level": 85,
  "is_charging": false,
  "timezone": "Asia/Shanghai",
  "lang": "zh-CN",
  "dpi": 460
}
```

### app_context 结构

```json
{
  "app_version": "7.45.0",
  "build_number": 74500000,
  "channel": "AppStore",
  "platform": "ios",
  "ab_test_groups": ["exp_123", "exp_456"],
  "push_enabled": true,
  "location_enabled": false,
  "sdk_version": "3.20.5"
}
```

---

## 埋点表详细示例

### 4. app_event_account（账号埋点）

#### account_register（注册）
```json
{
  "event_id": "account_register",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 90123456,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/account/register",
  "referer": "https://www.bilibili.com/",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "account_register",
    "source_page": "login",
    "register_type": "phone",
    "invite_code": "INV12345",
    "channel": "AppStore"
  }
}
```

#### account_login（登录）
```json
{
  "event_id": "account_login",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/account/login",
  "referer": "app://settings",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "account_login",
    "source_page": "app_start",
    "login_type": "fingerprint",
    "is_auto_login": true,
    "login_result": "success"
  }
}
```

#### account_logout（登出）
```json
{
  "event_id": "account_logout",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/account/logout",
  "referer": "app://settings",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "account_logout",
    "source_page": "settings"
  }
}
```

#### account_update_profile（修改资料）
```json
{
  "event_id": "account_update_profile",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/account/profile",
  "referer": "app://profile",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "account_update_profile",
    "source_page": "profile",
    "updated_fields": ["nick_name", "sign"],
    "before_snapshot": { "nick_name": "旧昵称" },
    "after_snapshot": { "nick_name": "新昵称" }
  }
}
```

#### account_update_password（修改密码）
```json
{
  "event_id": "account_update_password",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/account/password",
  "referer": "app://settings",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "account_update_password",
    "source_page": "settings"
  }
}
```

#### account_update_avatar（修改头像）
```json
{
  "event_id": "account_update_avatar",
  "properties": {
    "event_type": "account_update_avatar",
    "source_page": "profile",
    "avatar_source": "album",
    "file_size_kb": 2048
  }
}
```

#### account_deactivate（注销账号）
```json
{
  "event_id": "account_deactivate",
  "properties": {
    "event_type": "account_deactivate",
    "source_page": "settings",
    "reason": "不再使用",
    "account_age_days": 365
  }
}
```

#### account_bind_phone / account_bind_email（绑定手机/邮箱）
```json
{
  "event_id": "account_bind_phone",
  "properties": {
    "event_type": "account_bind_phone",
    "source_page": "settings",
    "bind_result": "success",
    "is_rebind": false
  }
}
```

---

### 5. app_event_video（视频埋点）

附加字段：`bvid`

#### video_view（观看视频）
```json
{
  "event_id": "video_view",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "bvid": "BV1abc12345",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345",
  "referer": "app://home",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "video_view",
    "bvid": "BV1abc12345",
    "from": "recommend",
    "spm_id": "333.788.0.0",
    "is_auto_play": false,
    "quality": "1080p"
  }
}
```

#### video_play_heartbeat（播放心跳）
```json
{
  "event_id": "video_play_heartbeat",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_play_heartbeat",
    "bvid": "BV1abc12345",
    "progress_sec": 120,
    "total_sec": 600,
    "play_speed": 1.5,
    "quality": "1080p",
    "is_full_screen": true
  }
}
```

#### video_upload_start（开始上传）
```json
{
  "event_id": "video_upload_start",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_upload_start",
    "file_size_mb": 500,
    "duration_sec": 600,
    "resolution": "4k",
    "upload_type": "single"
  }
}
```

#### video_upload_progress（上传进度）
```json
{
  "event_id": "video_upload_progress",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_upload_progress",
    "upload_id": "550e8400-e29b-41d4-a716-446655440000",
    "progress_percent": 45,
    "uploaded_mb": 225,
    "speed_kbps": 10000
  }
}
```

#### video_upload_complete（上传完成）
```json
{
  "event_id": "video_upload_complete",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_upload_complete",
    "bvid": "BV1abc12345",
    "title": "新上传视频_abc123",
    "category": "game",
    "duration_sec": 600,
    "upload_duration_sec": 120
  }
}
```

#### video_upload_fail（上传失败）
```json
{
  "event_id": "video_upload_fail",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_upload_fail",
    "error_code": "NETWORK_ERROR",
    "error_msg": "上传失败",
    "retry_count": 2
  }
}
```

#### video_like / video_unlike（点赞/取消点赞）
```json
{
  "event_id": "video_like",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_like",
    "bvid": "BV1abc12345",
    "from": "recommend",
    "is_liked_before": false
  }
}
```

#### video_coin（投币）
```json
{
  "event_id": "video_coin",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_coin",
    "bvid": "BV1abc12345",
    "coin_count": 2,
    "with_like": true,
    "remain_coins": 100
  }
}
```

#### video_favorite / video_unfavorite（收藏/取消收藏）
```json
{
  "event_id": "video_favorite",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_favorite",
    "bvid": "BV1abc12345",
    "fav_folder_ids": [1, 5, 10],
    "create_new_folder": false
  }
}
```

#### video_share（分享）
```json
{
  "event_id": "video_share",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_share",
    "bvid": "BV1abc12345",
    "share_channel": "wechat",
    "share_result": "success"
  }
}
```

#### video_danmaku（发弹幕）
```json
{
  "event_id": "video_danmaku",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_danmaku",
    "bvid": "BV1abc12345",
    "content": "前方高能",
    "progress_sec": 120,
    "danmaku_type": "scroll",
    "color": "#FFFFFF"
  }
}
```

#### video_triple（一键三连）
```json
{
  "event_id": "video_triple",
  "bvid": "BV1abc12345",
  "properties": {
    "event_type": "video_triple",
    "bvid": "BV1abc12345",
    "from": "video_page",
    "coin_count": 2
  }
}
```

#### video_update_info（修改视频信息）
```json
{
  "event_id": "video_update_info",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "bvid": "BV1abc12345",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345",
  "referer": "app://creative_center",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "video_update_info",
    "bvid": "BV1abc12345",
    "updated_fields": ["title", "desc", "tags"]
  }
}
```

#### video_delete（删除视频）
```json
{
  "event_id": "video_delete",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "bvid": "BV1abc12345",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345",
  "referer": "app://creative_center",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "video_delete",
    "bvid": "BV1abc12345",
    "delete_reason": "personal",
    "video_age_days": 30
  }
}
```

#### video_set_private（设为私密）
```json
{
  "event_id": "video_set_private",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "bvid": "BV1abc12345",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345",
  "referer": "app://creative_center",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "video_set_private"
  }
}
```

#### video_publish（发布/公开）
```json
{
  "event_id": "video_publish",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "bvid": "BV1abc12345",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345",
  "referer": "app://creative_center",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "video_publish"
  }
}
```

---

### 6. app_event_social（社交互动埋点）

附加字段：`target_mid`

#### social_follow（关注）
```json
{
  "event_id": "social_follow",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "target_mid": 87654321,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/space/87654321",
  "referer": "app://video/BV1xxx",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "social_follow",
    "target_mid": 87654321,
    "from": "video",
    "target_level": 6,
    "target_follower_count": 500000,
    "is_mutual": false,
    "special_group": "特别关注"
  }
}
```

#### social_unfollow（取消关注）
```json
{
  "event_id": "social_unfollow",
  "target_mid": 87654321,
  "properties": {
    "event_type": "social_unfollow",
    "target_mid": 87654321,
    "from": "space",
    "follow_duration_days": 180,
    "unfollow_reason": "not_interested"
  }
}
```

#### social_block（拉黑）
```json
{
  "event_id": "social_block",
  "target_mid": 87654321,
  "properties": {
    "event_type": "social_block",
    "target_mid": 87654321,
    "from": "comment",
    "block_reason": "harassment",
    "is_following": false
  }
}
```

#### social_unblock（取消拉黑）
```json
{
  "event_id": "social_unblock",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "target_mid": 87654321,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/space/87654321",
  "referer": "app://settings/blacklist",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "social_unblock",
    "target_mid": 87654321,
    "from": "settings"
  }
}
```

#### social_whisper（私信）
```json
{
  "event_id": "social_whisper",
  "target_mid": 87654321,
  "properties": {
    "event_type": "social_whisper",
    "target_mid": 87654321,
    "from": "space",
    "msg_type": "text",
    "msg_length": 50,
    "is_first_msg": true
  }
}
```

#### social_report_user（举报用户）
```json
{
  "event_id": "social_report_user",
  "target_mid": 87654321,
  "properties": {
    "event_type": "social_report_user",
    "target_mid": 87654321,
    "from": "space",
    "report_reason": "spam",
    "report_evidence": "screenshot"
  }
}
```

---

### 7. app_event_comment（评论埋点）

附加字段：`oid`（视频BV号）, `rpid`（评论ID）

#### comment_create（发表评论）
```json
{
  "event_id": "comment_create",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/video/BV1abc12345#reply",
  "referer": "app://video/BV1abc12345",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "comment_create",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "content": "太强了！",
    "content_length": 4,
    "has_emoji": false,
    "has_at": false,
    "from": "video_page"
  }
}
```

#### comment_reply（回复评论）
```json
{
  "event_id": "comment_reply",
  "oid": "BV1abc12345",
  "rpid": 123456790,
  "properties": {
    "event_type": "comment_reply",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456790,
    "root_rpid": 123456789,
    "parent_rpid": 123456789,
    "content": "同意！",
    "content_length": 3,
    "reply_to_mid": 87654321
  }
}
```

#### comment_update（修改评论）
```json
{
  "event_id": "comment_update",
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "properties": {
    "event_type": "comment_update",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "old_content": "原评论内容",
    "new_content": "修改后的内容",
    "edit_reason": "typo"
  }
}
```

#### comment_delete（删除评论）
```json
{
  "event_id": "comment_delete",
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "properties": {
    "event_type": "comment_delete",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "delete_reason": "self_delete",
    "comment_age_hours": 24
  }
}
```

#### comment_report（举报评论）
```json
{
  "event_id": "comment_report",
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "properties": {
    "event_type": "comment_report",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "report_reason": "spam",
    "report_content_preview": "被举报的内容..."
  }
}
```

#### comment_like / comment_unlike（评论点赞/取消点赞）
```json
{
  "event_id": "comment_like",
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "properties": {
    "event_type": "comment_like",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "is_root_comment": true,
    "comment_owner_mid": 87654321
  }
}
```

#### comment_dislike / comment_undislike（评论点踩/取消点踩）
```json
{
  "event_id": "comment_dislike",
  "oid": "BV1abc12345",
  "rpid": 123456789,
  "properties": {
    "event_type": "comment_dislike",
    "oid": "BV1abc12345",
    "otype": 1,
    "rpid": 123456789,
    "is_root_comment": true
  }
}
```

---

### 8. app_event_vip（VIP购买埋点）

附加字段：`order_no`（订单号，部分事件）

#### vip_page_view（浏览VIP页）
```json
{
  "event_id": "vip_page_view",
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "mid": 12345678,
  "order_no": "",
  "client_ts": 1705987200000,
  "server_ts": 1705987200300,
  "url_path": "/vip/buy",
  "referer": "app://home",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) BiliApp/7.45.0",
  "ip": "192.168.1.100",
  "device_info": { "..." },
  "app_context": { "..." },
  "properties": {
    "event_type": "vip_page_view",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "from": "home_banner",
    "page_load_ms": 500,
    "is_promotion_period": true
  }
}
```

#### vip_select_plan（选择套餐）
```json
{
  "event_id": "vip_select_plan",
  "order_no": "",
  "properties": {
    "event_type": "vip_select_plan",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "plan_id": "yearly",
    "plan_name": "大会员年卡",
    "plan_price": 23800,
    "plan_duration_days": 366,
    "has_coupon": true
  }
}
```

#### vip_create_order（创建订单）
```json
{
  "event_id": "vip_create_order",
  "order_no": "BP170598720012345",
  "properties": {
    "event_type": "vip_create_order",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "order_no": "BP170598720012345",
    "plan_id": "yearly",
    "plan_name": "大会员年卡",
    "original_price": 23800,
    "final_price": 19040,
    "coupon_id": "CPN12345",
    "discount_amount": 476
  }
}
```

#### vip_pay_start（开始支付）
```json
{
  "event_id": "vip_pay_start",
  "order_no": "BP170598720012345",
  "properties": {
    "event_type": "vip_pay_start",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "order_no": "BP170598720012345",
    "pay_method": "alipay",
    "amount": 19040
  }
}
```

#### vip_pay_success（支付成功）
```json
{
  "event_id": "vip_pay_success",
  "order_no": "BP170598720012345",
  "properties": {
    "event_type": "vip_pay_success",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "order_no": "BP170598720012345",
    "pay_method": "alipay",
    "amount": 19040,
    "pay_duration_sec": 30,
    "new_vip_expire": 1737609600
  }
}
```

#### vip_pay_fail（支付失败）
```json
{
  "event_id": "vip_pay_fail",
  "order_no": "BP170598720012345",
  "properties": {
    "event_type": "vip_pay_fail",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "order_no": "BP170598720012345",
    "pay_method": "wechat",
    "error_code": "INSUFFICIENT_BALANCE",
    "error_msg": "支付失败",
    "retry_count": 1
  }
}
```

#### vip_pay_cancel（取消支付）
```json
{
  "event_id": "vip_pay_cancel",
  "order_no": "BP170598720012345",
  "properties": {
    "event_type": "vip_pay_cancel",
    "current_vip_type": 0,
    "current_vip_expire": 0,
    "order_no": "BP170598720012345",
    "cancel_stage": "during_pay",
    "time_on_pay_page_sec": 60
  }
}
```

#### vip_auto_renew_on / vip_auto_renew_off（开启/关闭自动续费）
```json
{
  "event_id": "vip_auto_renew_on",
  "order_no": "",
  "properties": {
    "event_type": "vip_auto_renew_on",
    "current_vip_type": 2,
    "current_vip_expire": 1737609600,
    "plan_id": "auto_yearly",
    "from": "vip_page",
    "previous_auto_renew": false
  }
}
```

# 度量值类型标注（增量 vs 全量）

## 业务表

### 1. app_user_profile（用户表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `level` | **全量** | 当前等级状态快照 |
| `coins` | **全量** | 当前硬币余额快照 |
| `vip_type` | **全量** | 当前VIP状态快照 |

---

### 2. app_video_content（视频表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `duration` | **全量** | 视频固定属性，不变 |
| `stats.view` | **全量** | 累计总播放量 |
| `stats.danmaku` | **全量** | 累计总弹幕数 |
| `stats.reply` | **全量** | 累计总评论数 |
| `stats.favorite` | **全量** | 累计总收藏数 |
| `stats.coin` | **全量** | 累计总投币数 |
| `stats.share` | **全量** | 累计总分享数 |
| `stats.like` | **全量** | 累计总点赞数 |

---

### 3. app_comment（评论表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `like_count` | **全量** | 累计总点赞数 |
| `dislike_count` | **全量** | 累计总点踩数 |
| `reply_count` | **全量** | 累计总回复数 |

---

## 埋点表

> ⚠️ **注意**：埋点表的每条记录本身就是**增量事件**，下表标注的是 `properties` 中度量字段的语义。

---

### 4. app_event_account（账号埋点）

| 事件 | 字段 | 类型 | 说明 |
|------|------|------|------|
| `account_update_avatar` | `file_size_kb` | **增量** | 本次上传文件大小 |
| `account_deactivate` | `account_age_days` | **全量** | 账号存续天数（计算得出的快照） |

---

### 5. app_event_video（视频埋点）

| 事件 | 字段 | 类型 | 说明 |
|------|------|------|------|
| `video_view` | - | - | 无度量值 |
| `video_play_heartbeat` | `progress_sec` | **增量** | 当前播放进度（瞬时值） |
| `video_play_heartbeat` | `total_sec` | **全量** | 视频总时长（固定值） |
| `video_upload_start` | `file_size_mb` | **全量** | 文件总大小（固定值） |
| `video_upload_start` | `duration_sec` | **全量** | 视频总时长（固定值） |
| `video_upload_progress` | `progress_percent` | **增量** | 当前上传进度（瞬时值） |
| `video_upload_progress` | `uploaded_mb` | **增量** | 已上传大小（累计瞬时值） |
| `video_upload_progress` | `speed_kbps` | **增量** | 当前上传速度（瞬时值） |
| `video_upload_complete` | `duration_sec` | **全量** | 视频总时长（固定值） |
| `video_upload_complete` | `upload_duration_sec` | **增量** | 本次上传耗时 |
| `video_upload_fail` | `retry_count` | **增量** | 累计重试次数（事件发生时的累计值） |
| `video_coin` | `coin_count` | **增量** | 本次投币数量（1或2） |
| `video_coin` | `remain_coins` | **全量** | 用户当前硬币余额快照 |
| `video_delete` | `video_age_days` | **全量** | 视频存续天数（计算得出的快照） |
| `video_triple` | `coin_count` | **增量** | 本次投币数量（固定为2） |

---

### 6. app_event_social（社交互动埋点）

| 事件 | 字段 | 类型 | 说明 |
|------|------|------|------|
| `social_follow` | `target_follower_count` | **全量** | 目标用户当前粉丝数快照 |
| `social_follow` | `target_level` | **全量** | 目标用户当前等级快照 |
| `social_unfollow` | `follow_duration_days` | **增量** | 本次关注持续天数 |
| `social_whisper` | `msg_length` | **增量** | 本条消息长度 |

---

### 7. app_event_comment（评论埋点）

| 事件 | 字段 | 类型 | 说明 |
|------|------|------|------|
| `comment_create` | `content_length` | **增量** | 本条评论长度 |
| `comment_reply` | `content_length` | **增量** | 本条回复长度 |
| `comment_delete` | `comment_age_hours` | **增量** | 评论存续小时数 |

---

### 8. app_event_vip（VIP购买埋点）

| 事件 | 字段 | 类型 | 说明 |
|------|------|------|------|
| `vip_page_view` | `page_load_ms` | **增量** | 本次页面加载耗时 |
| `vip_page_view` | `current_vip_type` | **全量** | 用户当前VIP类型快照 |
| `vip_page_view` | `current_vip_expire` | **全量** | 用户当前VIP到期时间快照 |
| `vip_select_plan` | `plan_price` | **全量** | 套餐价格（固定值） |
| `vip_select_plan` | `plan_duration_days` | **全量** | 套餐时长（固定值） |
| `vip_create_order` | `original_price` | **全量** | 套餐原价（固定值） |
| `vip_create_order` | `final_price` | **增量** | 本次订单实付金额 |
| `vip_create_order` | `discount_amount` | **增量** | 本次订单折扣金额 |
| `vip_pay_start` | `amount` | **增量** | 本次支付金额 |
| `vip_pay_success` | `amount` | **增量** | 本次支付金额 |
| `vip_pay_success` | `pay_duration_sec` | **增量** | 本次支付耗时 |
| `vip_pay_success` | `new_vip_expire` | **全量** | 更新后的VIP到期时间快照 |
| `vip_pay_fail` | `retry_count` | **增量** | 累计重试次数 |
| `vip_pay_cancel` | `time_on_pay_page_sec` | **增量** | 本次在支付页停留时长 |
