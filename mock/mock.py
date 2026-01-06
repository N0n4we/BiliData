from kafka import KafkaProducer
import random
import time
import uuid
import json
import socket
import struct
from decimal import Decimal
from typing import Dict, Any, List

# ==========================================
# 1. 配置区域 (Configuration)
# ==========================================
class Constant:
    # Kafka 配置
    KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

    # 业务表 (作为 Kafka Topic)
    TBL_USER = "app_user_profile"
    TBL_VIDEO = "app_video_content"
    TBL_COMMENT = "app_comment"

    # 埋点表 - 按主题分表 (作为 Kafka Topic)
    TBL_EVENT_ACCOUNT = "app_event_account"        # 账号相关埋点
    TBL_EVENT_VIDEO = "app_event_video"            # 视频相关埋点
    TBL_EVENT_SOCIAL = "app_event_social"          # 社交互动埋点
    TBL_EVENT_COMMENT = "app_event_comment"        # 评论相关埋点
    TBL_EVENT_VIP = "app_event_vip"                # VIP购买埋点


# ==========================================
# 2. 埋点事件类型定义
# ==========================================
class EventTypes:
    # 账号相关事件
    ACCOUNT_REGISTER = "account_register"           # 账号注册
    ACCOUNT_LOGIN = "account_login"                 # 账号登录
    ACCOUNT_LOGOUT = "account_logout"               # 账号登出
    ACCOUNT_UPDATE_PROFILE = "account_update_profile"  # 修改资料
    ACCOUNT_UPDATE_AVATAR = "account_update_avatar"    # 修改头像
    ACCOUNT_UPDATE_PASSWORD = "account_update_password" # 修改密码
    ACCOUNT_DEACTIVATE = "account_deactivate"       # 注销账号
    ACCOUNT_BIND_PHONE = "account_bind_phone"       # 绑定手机
    ACCOUNT_BIND_EMAIL = "account_bind_email"       # 绑定邮箱

    # 视频相关事件
    VIDEO_UPLOAD_START = "video_upload_start"       # 开始上传
    VIDEO_UPLOAD_PROGRESS = "video_upload_progress" # 上传进度
    VIDEO_UPLOAD_COMPLETE = "video_upload_complete" # 上传完成
    VIDEO_UPLOAD_FAIL = "video_upload_fail"         # 上传失败
    VIDEO_UPDATE_INFO = "video_update_info"         # 修改视频信息
    VIDEO_DELETE = "video_delete"                   # 删除视频
    VIDEO_SET_PRIVATE = "video_set_private"         # 设为私密
    VIDEO_PUBLISH = "video_publish"                 # 发布/公开
    VIDEO_VIEW = "video_view"                       # 观看视频
    VIDEO_PLAY_HEARTBEAT = "video_play_heartbeat"   # 播放心跳

    # 账号互动事件
    SOCIAL_FOLLOW = "social_follow"                 # 关注
    SOCIAL_UNFOLLOW = "social_unfollow"             # 取消关注
    SOCIAL_BLOCK = "social_block"                   # 拉黑
    SOCIAL_UNBLOCK = "social_unblock"               # 取消拉黑
    SOCIAL_WHISPER = "social_whisper"               # 私信
    SOCIAL_REPORT_USER = "social_report_user"       # 举报用户

    # 视频互动事件
    VIDEO_LIKE = "video_like"                       # 点赞
    VIDEO_UNLIKE = "video_unlike"                   # 取消点赞
    VIDEO_COIN = "video_coin"                       # 投币
    VIDEO_FAVORITE = "video_favorite"               # 收藏
    VIDEO_UNFAVORITE = "video_unfavorite"           # 取消收藏
    VIDEO_SHARE = "video_share"                     # 分享
    VIDEO_DANMAKU = "video_danmaku"                 # 发送弹幕
    VIDEO_TRIPLE = "video_triple"                   # 一键三连

    # 评论相关事件
    COMMENT_CREATE = "comment_create"               # 发表评论
    COMMENT_REPLY = "comment_reply"                 # 回复评论
    COMMENT_UPDATE = "comment_update"               # 修改评论
    COMMENT_DELETE = "comment_delete"               # 删除评论
    COMMENT_REPORT = "comment_report"               # 举报评论

    # 评论互动事件
    COMMENT_LIKE = "comment_like"                   # 评论点赞
    COMMENT_UNLIKE = "comment_unlike"               # 取消评论点赞
    COMMENT_DISLIKE = "comment_dislike"             # 评论点踩
    COMMENT_UNDISLIKE = "comment_undislike"         # 取消评论点踩

    # VIP相关事件
    VIP_PAGE_VIEW = "vip_page_view"                 # 浏览VIP页面
    VIP_SELECT_PLAN = "vip_select_plan"             # 选择套餐
    VIP_CREATE_ORDER = "vip_create_order"           # 创建订单
    VIP_PAY_START = "vip_pay_start"                 # 开始支付
    VIP_PAY_SUCCESS = "vip_pay_success"             # 支付成功
    VIP_PAY_FAIL = "vip_pay_fail"                   # 支付失败
    VIP_PAY_CANCEL = "vip_pay_cancel"               # 取消支付
    VIP_AUTO_RENEW_ON = "vip_auto_renew_on"         # 开启自动续费
    VIP_AUTO_RENEW_OFF = "vip_auto_renew_off"       # 关闭自动续费


# ==========================================
# 3. 数据工具类 (MockDataUtil)
# ==========================================
class MockDataUtil:
    SURNAMES = ["王", "李", "张", "刘", "陈", "杨", "黄", "赵", "吴", "周", "徐", "孙", "马", "朱", "胡", "郭", "林", "何"]
    GIVEN_NAMES = ["伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋", "勇", "杰", "明", "秀英", "欣", "雪", "浩", "涛"]

    USER_AGENTS = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 BiliApp/7.34.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 BiliApp/7.45.0",
        "Mozilla/5.0 (Linux; Android 13; SM-S9180) AppleWebKit/537.36 Chrome/113.0.5672.162 Mobile Safari/537.36 BiliApp/7.32.0",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 Chrome/119.0.0.0 Mobile Safari/537.36 BiliApp/7.50.0",
        "Mozilla/5.0 (Linux; Android 13; Xiaomi 13 Ultra) AppleWebKit/537.36 Chrome/118.0.0.0 Mobile Safari/537.36 BiliApp/7.48.0",
        "Bilibili/7.35.0 (iPhone; iOS 16.5; Scale/3.00)",
        "Bilibili/7.50.0 (Android; 14; Scale/2.75)"
    ]

    CHANNELS = ["AppStore", "Xiaomi", "Huawei", "Oppo", "Vivo", "Tencent", "Official", "GooglePlay", "Samsung", "Honor"]
    NET_TYPES = ["WIFI", "5G", "4G", "3G", "Unknown", "WIFI_5G", "WIFI_2.4G"]
    PLATFORMS = ["ios", "android", "android_hd", "ipad"]
    PAGE_SOURCES = ["homepage", "search", "recommend", "history", "favorites", "space", "hot", "rank", "notification"]
    SHARE_CHANNELS = ["wechat", "wechat_moments", "qq", "weibo", "copy_link", "system_share"]
    REPORT_REASONS = ["spam", "harassment", "hate_speech", "violence", "copyright", "inappropriate", "other"]
    VIDEO_CATEGORIES = ["douga", "anime", "guochuang", "music", "dance", "game", "tech", "life", "food", "animal", "car", "fashion", "ent", "movie", "tv"]

    COMMENT_TEMPLATES = [
        "太强了！", "学到了", "前排围观", "火钳刘明", "下次一定", "太好看了", "不愧是你", "awsl",
        "泪目了", "笑死我了", "建议三连", "第一次来，点个关注", "催更催更", "爷青回", "YYDS",
        "这也太绝了", "DNA动了", "直接封神", "绷不住了", "大佬带带我", "建议全部看完再说话"
    ]

    VIP_PLANS = [
        {"id": "monthly", "name": "大会员月卡", "price": 2500, "duration_days": 31},
        {"id": "quarterly", "name": "大会员季卡", "price": 6800, "duration_days": 93},
        {"id": "yearly", "name": "大会员年卡", "price": 23800, "duration_days": 366},
        {"id": "auto_monthly", "name": "连续包月大会员", "price": 1500, "duration_days": 31},
        {"id": "auto_yearly", "name": "连续包年大会员", "price": 17800, "duration_days": 366}
    ]

    PAY_METHODS = ["alipay", "wechat", "apple_iap", "google_iap", "bp_balance", "bank_card"]

    @staticmethod
    def get_current_ts():
        return int(time.time() * 1000)

    @staticmethod
    def generate_trace_id():
        return str(uuid.uuid4()).replace("-", "")

    @staticmethod
    def generate_session_id():
        return str(uuid.uuid4())

    @staticmethod
    def generate_bvid():
        return f"BV1{uuid.uuid4().hex[:9]}"

    @staticmethod
    def generate_comment_id():
        return random.randint(100000000, 999999999)

    @staticmethod
    def generate_order_no():
        return f"BP{int(time.time())}{random.randint(10000, 99999)}"

    @staticmethod
    def generate_ip():
        return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))

    @classmethod
    def generate_username(cls):
        return random.choice(cls.SURNAMES) + random.choice(cls.GIVEN_NAMES) + str(random.randint(0, 99))

    @classmethod
    def generate_device_info(cls) -> Dict[str, Any]:
        is_ios = random.choice([True, False])
        return {
            "device_id": str(uuid.uuid4()),
            "idfa": str(uuid.uuid4()) if is_ios else "",
            "oaid": str(uuid.uuid4()) if not is_ios else "",
            "android_id": "" if is_ios else str(uuid.uuid4()).replace("-", "")[:16],
            "brand": "Apple" if is_ios else random.choice(["Xiaomi", "Huawei", "Samsung", "Oppo", "Vivo", "OnePlus", "Google"]),
            "model": random.choice(["iPhone14,2", "iPhone15,2", "iPhone15,3"]) if is_ios else random.choice(["M2012K11C", "SM-S9180", "Pixel 8", "22081212C"]),
            "os": "iOS" if is_ios else "Android",
            "os_version": random.choice(["16.1", "16.5", "17.0", "17.1"]) if is_ios else random.choice(["12", "13", "14"]),
            "screen_width": random.choice([1080, 1170, 1284, 1440]),
            "screen_height": random.choice([2340, 2532, 2778, 3200]),
            "carrier": random.choice(["ChinaMobile", "ChinaUnicom", "ChinaTelecom", "Unknown"]),
            "network": random.choice(cls.NET_TYPES),
            "battery_level": random.randint(10, 100),
            "is_charging": random.choice([True, False]),
            "timezone": "Asia/Shanghai",
            "lang": random.choice(["zh-CN", "zh-TW", "zh-HK"]),
            "dpi": random.choice([320, 420, 480, 560])
        }

    @classmethod
    def generate_app_context(cls):
        major = random.randint(7, 8)
        minor = random.randint(30, 55)
        patch = random.randint(0, 5)
        return {
            "app_version": f"{major}.{minor}.{patch}",
            "build_number": random.randint(73000000, 85000000),
            "channel": random.choice(cls.CHANNELS),
            "platform": random.choice(cls.PLATFORMS),
            "ab_test_groups": [f"exp_{random.randint(100, 500)}" for _ in range(random.randint(1, 5))],
            "push_enabled": random.choice([True, False]),
            "location_enabled": random.choice([True, False]),
            "sdk_version": f"3.{random.randint(10, 30)}.{random.randint(0, 10)}"
        }


# ==========================================
# 4. 数据生成器 (Kafka)
# ==========================================
class ODSGenerator:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self.mids: List[int] = []
        self.bvids: List[str] = []
        self.rpids: List[int] = []

    def _send_to_kafka(self, topic: str, messages: List[Dict]):
        """批量发送消息到 Kafka"""
        for msg in messages:
            self.producer.send(topic, value=msg)
        self.producer.flush()

    def _get_base_event_data(self, mid: int):
        """生成埋点基础数据"""
        client_ts = MockDataUtil.get_current_ts() - random.randint(0, 60000)
        return {
            "trace_id": MockDataUtil.generate_trace_id(),
            "session_id": MockDataUtil.generate_session_id(),
            "mid": mid,
            "client_ts": client_ts,
            "server_ts": client_ts + random.randint(50, 500),
            "ua": random.choice(MockDataUtil.USER_AGENTS),
            "ip": MockDataUtil.generate_ip(),
            "device_info": MockDataUtil.generate_device_info(),
            "app_context": MockDataUtil.generate_app_context()
        }

    # ==========================================
    # 基础数据生成
    # ==========================================
    def gen_users(self, count: int):
        """生成基础用户数据"""
        data = []
        for _ in range(count):
            mid = random.randint(10000000, 99999999)
            self.mids.append(mid)

            verify_json = {
                "type": random.choice([-1, 0, 1]),
                "desc": "bilibili 知名UP主" if random.random() > 0.95 else ""
            }

            settings_json = {
                "privacy": {"show_fav": random.choice([True, False]), "show_history": random.choice([True, False])},
                "push": {"comment": True, "like": random.choice([True, False]), "at": True},
                "theme": random.choice(["auto", "light", "dark"])
            }

            tags = random.sample(["宅", "萌", "技术宅", "古风", "鬼畜", "一般路过", "游戏", "动漫", "音乐", "舞蹈"], k=random.randint(0, 4))

            data.append({
                "mid": mid,
                "nick_name": MockDataUtil.generate_username(),
                "sex": random.choice(["男", "女", "保密"]),
                "face_url": f"https://i0.hdslb.com/bfs/face/{uuid.uuid4().hex}.jpg",
                "sign": random.choice(["这个人很懒", "我的简介？不存在的", "二次元浓度超标", "干杯！", ""]),
                "level": random.randint(0, 6),
                "birthday": f"{random.randint(1985, 2008)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "coins": round(random.uniform(0, 5000), 1),
                "vip_type": random.choice([0, 0, 0, 1, 2]),
                "official_verify": verify_json,
                "settings": settings_json,
                "tags": tags,
                "status": 0,
                "created_at": int(time.time() * 1000),
                "updated_at": int(time.time() * 1000)
            })

        self._send_to_kafka(Constant.TBL_USER, data)
        print(f"Generated {count} users -> topic: {Constant.TBL_USER}")

    def gen_videos(self, count: int):
        """生成基础视频数据"""
        if not self.mids:
            return

        data = []
        titles = ["【原创】", "【转载】", "【自制】", "【搬运】", "【4K】", "【教程】", "【测评】", "【vlog】"]
        topics = ["这个视频太绝了", "年度最佳", "挑战不可能", "一看就会", "新手必看", "深度解析", "独家揭秘", "全网首发"]

        for _ in range(count):
            bvid = MockDataUtil.generate_bvid()
            self.bvids.append(bvid)
            mid = random.choice(self.mids)
            category = random.choice(MockDataUtil.VIDEO_CATEGORIES)

            meta = {
                "upload_ip": MockDataUtil.generate_ip(),
                "camera": random.choice(["Sony A7M4", "iPhone 15 Pro", "Canon R5", "GoPro 12", "DJI Pocket 3"]),
                "software": random.choice(["Premiere Pro", "Final Cut Pro", "DaVinci Resolve", "剪映"]),
                "resolution": random.choice(["1080p", "2k", "4k"]),
                "fps": random.choice([24, 30, 60, 120])
            }

            stats = {
                "view": random.randint(0, 1000000),
                "danmaku": random.randint(0, 10000),
                "reply": random.randint(0, 5000),
                "favorite": random.randint(0, 20000),
                "coin": random.randint(0, 30000),
                "share": random.randint(0, 5000),
                "like": random.randint(0, 50000)
            }

            audit = [
                {"ts": int(time.time()) - random.randint(0, 86400), "operator": "system", "status": "pass"},
            ]

            pubdate = int(time.time() * 1000) - random.randint(0, 365) * 86400 * 1000

            data.append({
                "bvid": bvid,
                "title": f"{random.choice(titles)}{random.choice(topics)}_{bvid[-6:]}",
                "cover": f"https://i0.hdslb.com/bfs/archive/{uuid.uuid4().hex}.jpg",
                "desc_text": "视频简介...",
                "duration": random.randint(30, 7200),
                "pubdate": pubdate,
                "mid": mid,
                "category_id": random.randint(1, 200),
                "category_name": category,
                "state": 0,
                "attribute": 0,
                "is_private": False,
                "meta_info": meta,
                "stats": stats,
                "audit_info": audit,
                "created_at": int(time.time() * 1000),
                "updated_at": int(time.time() * 1000)
            })

        self._send_to_kafka(Constant.TBL_VIDEO, data)
        print(f"Generated {count} videos -> topic: {Constant.TBL_VIDEO}")

    def gen_comments(self, count: int):
        """生成基础评论数据"""
        if not self.mids or not self.bvids:
            return

        data = []
        for _ in range(count):
            rpid = MockDataUtil.generate_comment_id()
            self.rpids.append(rpid)

            is_reply = random.random() > 0.7 and len(self.rpids) > 10
            root = random.choice(self.rpids[:-1]) if is_reply else 0

            created_at = int(time.time() * 1000) - random.randint(0, 720) * 3600 * 1000

            data.append({
                "rpid": rpid,
                "oid": random.choice(self.bvids),
                "otype": 1,
                "mid": random.choice(self.mids),
                "root": root,
                "parent": root,
                "content": random.choice(MockDataUtil.COMMENT_TEMPLATES),
                "like_count": random.randint(0, 1000),
                "dislike_count": 0,
                "reply_count": random.randint(0, 50) if not is_reply else 0,
                "state": 0,
                "created_at": created_at,
                "updated_at": created_at
            })

        self._send_to_kafka(Constant.TBL_COMMENT, data)
        print(f"Generated {count} comments -> topic: {Constant.TBL_COMMENT}")

    # ==========================================
    # 账号相关埋点生成
    # ==========================================
    def gen_account_events(self, count: int):
        """生成账号相关埋点: 创建、修改、删除"""
        if not self.mids:
            return

        events = [
            (EventTypes.ACCOUNT_REGISTER, 10),
            (EventTypes.ACCOUNT_LOGIN, 30),
            (EventTypes.ACCOUNT_LOGOUT, 15),
            (EventTypes.ACCOUNT_UPDATE_PROFILE, 15),
            (EventTypes.ACCOUNT_UPDATE_AVATAR, 8),
            (EventTypes.ACCOUNT_UPDATE_PASSWORD, 5),
            (EventTypes.ACCOUNT_DEACTIVATE, 2),
            (EventTypes.ACCOUNT_BIND_PHONE, 8),
            (EventTypes.ACCOUNT_BIND_EMAIL, 7),
        ]

        data = []
        for _ in range(count):
            event_type = random.choices([e[0] for e in events], weights=[e[1] for e in events])[0]
            mid = random.choice(self.mids) if event_type != EventTypes.ACCOUNT_REGISTER else random.randint(90000000, 99999999)
            base = self._get_base_event_data(mid)

            props = {"event_type": event_type, "source_page": random.choice(["settings", "profile", "login", "app_start"])}

            if event_type == EventTypes.ACCOUNT_REGISTER:
                props.update({
                    "register_type": random.choice(["phone", "email", "qq", "wechat", "weibo"]),
                    "invite_code": f"INV{random.randint(10000, 99999)}" if random.random() > 0.8 else "",
                    "channel": random.choice(MockDataUtil.CHANNELS)
                })
            elif event_type == EventTypes.ACCOUNT_LOGIN:
                props.update({
                    "login_type": random.choice(["password", "sms", "qrcode", "fingerprint", "face_id"]),
                    "is_auto_login": random.choice([True, False]),
                    "login_result": random.choices(["success", "fail"], weights=[95, 5])[0]
                })
            elif event_type == EventTypes.ACCOUNT_UPDATE_PROFILE:
                props.update({
                    "updated_fields": random.sample(["nick_name", "sign", "sex", "birthday"], k=random.randint(1, 3)),
                    "before_snapshot": {"nick_name": "旧昵称"},
                    "after_snapshot": {"nick_name": "新昵称"}
                })
            elif event_type == EventTypes.ACCOUNT_UPDATE_AVATAR:
                props.update({
                    "avatar_source": random.choice(["camera", "album", "system_preset"]),
                    "file_size_kb": random.randint(100, 5000)
                })
            elif event_type == EventTypes.ACCOUNT_DEACTIVATE:
                props.update({
                    "reason": random.choice(["不再使用", "隐私顾虑", "账号安全", "其他"]),
                    "account_age_days": random.randint(30, 2000)
                })
            elif event_type in [EventTypes.ACCOUNT_BIND_PHONE, EventTypes.ACCOUNT_BIND_EMAIL]:
                props.update({
                    "bind_result": random.choices(["success", "fail"], weights=[90, 10])[0],
                    "is_rebind": random.choice([True, False])
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": f"/account/{event_type.split('_')[-1]}",
                "referer": random.choice(["https://www.bilibili.com/", "app://settings", "app://profile"]),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_ACCOUNT, data)
        print(f"Generated {count} account events -> topic: {Constant.TBL_EVENT_ACCOUNT}")

    # ==========================================
    # 视频相关埋点生成
    # ==========================================
    def gen_video_events(self, count: int):
        """生成视频相关埋点: 上传、修改、删除、观看等"""
        if not self.mids:
            return

        events = [
            (EventTypes.VIDEO_VIEW, 30),
            (EventTypes.VIDEO_PLAY_HEARTBEAT, 25),
            (EventTypes.VIDEO_UPLOAD_START, 5),
            (EventTypes.VIDEO_UPLOAD_PROGRESS, 8),
            (EventTypes.VIDEO_UPLOAD_COMPLETE, 4),
            (EventTypes.VIDEO_UPLOAD_FAIL, 1),
            (EventTypes.VIDEO_UPDATE_INFO, 5),
            (EventTypes.VIDEO_DELETE, 2),
            (EventTypes.VIDEO_SET_PRIVATE, 2),
            (EventTypes.VIDEO_PUBLISH, 3),
            (EventTypes.VIDEO_LIKE, 8),
            (EventTypes.VIDEO_UNLIKE, 2),
            (EventTypes.VIDEO_COIN, 5),
            (EventTypes.VIDEO_FAVORITE, 6),
            (EventTypes.VIDEO_UNFAVORITE, 2),
            (EventTypes.VIDEO_SHARE, 4),
            (EventTypes.VIDEO_DANMAKU, 6),
            (EventTypes.VIDEO_TRIPLE, 3),
        ]

        data = []
        for _ in range(count):
            event_type = random.choices([e[0] for e in events], weights=[e[1] for e in events])[0]
            mid = random.choice(self.mids)
            bvid = random.choice(self.bvids) if self.bvids else MockDataUtil.generate_bvid()
            base = self._get_base_event_data(mid)

            props = {"event_type": event_type}

            if event_type == EventTypes.VIDEO_VIEW:
                props.update({
                    "bvid": bvid,
                    "from": random.choice(MockDataUtil.PAGE_SOURCES),
                    "spm_id": f"{random.randint(100,999)}.{random.randint(100,999)}.0.0",
                    "is_auto_play": random.choice([True, False]),
                    "quality": random.choice(["360p", "480p", "720p", "1080p", "4k"])
                })
            elif event_type == EventTypes.VIDEO_PLAY_HEARTBEAT:
                props.update({
                    "bvid": bvid,
                    "progress_sec": random.randint(1, 3600),
                    "total_sec": random.randint(60, 7200),
                    "play_speed": random.choice([0.5, 1.0, 1.25, 1.5, 2.0]),
                    "quality": random.choice(["720p", "1080p", "4k"]),
                    "is_full_screen": random.choice([True, False])
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_START:
                props.update({
                    "file_size_mb": random.randint(10, 5000),
                    "duration_sec": random.randint(30, 7200),
                    "resolution": random.choice(["1080p", "2k", "4k"]),
                    "upload_type": random.choice(["single", "multi_part"])
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_PROGRESS:
                props.update({
                    "upload_id": str(uuid.uuid4()),
                    "progress_percent": random.randint(1, 99),
                    "uploaded_mb": random.randint(1, 1000),
                    "speed_kbps": random.randint(500, 50000)
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_COMPLETE:
                props.update({
                    "bvid": bvid,
                    "title": f"新上传视频_{bvid[-6:]}",
                    "category": random.choice(MockDataUtil.VIDEO_CATEGORIES),
                    "duration_sec": random.randint(30, 7200),
                    "upload_duration_sec": random.randint(10, 600)
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_FAIL:
                props.update({
                    "error_code": random.choice(["NETWORK_ERROR", "FILE_TOO_LARGE", "FORMAT_NOT_SUPPORT", "SERVER_ERROR"]),
                    "error_msg": "上传失败",
                    "retry_count": random.randint(0, 3)
                })
            elif event_type == EventTypes.VIDEO_UPDATE_INFO:
                props.update({
                    "bvid": bvid,
                    "updated_fields": random.sample(["title", "desc", "cover", "tags", "category"], k=random.randint(1, 3))
                })
            elif event_type == EventTypes.VIDEO_DELETE:
                props.update({
                    "bvid": bvid,
                    "delete_reason": random.choice(["personal", "copyright", "mistake", "other"]),
                    "video_age_days": random.randint(1, 365)
                })
            elif event_type in [EventTypes.VIDEO_LIKE, EventTypes.VIDEO_UNLIKE]:
                props.update({
                    "bvid": bvid,
                    "from": random.choice(MockDataUtil.PAGE_SOURCES),
                    "is_liked_before": event_type == EventTypes.VIDEO_UNLIKE
                })
            elif event_type == EventTypes.VIDEO_COIN:
                props.update({
                    "bvid": bvid,
                    "coin_count": random.choice([1, 2]),
                    "with_like": random.choice([True, False]),
                    "remain_coins": random.randint(0, 500)
                })
            elif event_type in [EventTypes.VIDEO_FAVORITE, EventTypes.VIDEO_UNFAVORITE]:
                props.update({
                    "bvid": bvid,
                    "fav_folder_ids": [random.randint(1, 100) for _ in range(random.randint(1, 3))],
                    "create_new_folder": random.choice([True, False]) if event_type == EventTypes.VIDEO_FAVORITE else False
                })
            elif event_type == EventTypes.VIDEO_SHARE:
                props.update({
                    "bvid": bvid,
                    "share_channel": random.choice(MockDataUtil.SHARE_CHANNELS),
                    "share_result": random.choices(["success", "cancel", "fail"], weights=[70, 25, 5])[0]
                })
            elif event_type == EventTypes.VIDEO_DANMAKU:
                props.update({
                    "bvid": bvid,
                    "content": random.choice(["哈哈哈", "awsl", "前方高能", "泪目", "太强了"]),
                    "progress_sec": random.randint(1, 600),
                    "danmaku_type": random.choice(["scroll", "top", "bottom"]),
                    "color": random.choice(["#FFFFFF", "#FE0302", "#FFFF00"])
                })
            elif event_type == EventTypes.VIDEO_TRIPLE:
                props.update({
                    "bvid": bvid,
                    "from": random.choice(MockDataUtil.PAGE_SOURCES),
                    "coin_count": 2
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "bvid": bvid,
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": f"/video/{bvid}",
                "referer": random.choice(["https://www.bilibili.com/", f"app://video/{bvid}", "app://home"]),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_VIDEO, data)
        print(f"Generated {count} video events -> topic: {Constant.TBL_EVENT_VIDEO}")

    # ==========================================
    # 社交互动埋点生成
    # ==========================================
    def gen_social_events(self, count: int):
        """生成账号互动埋点: 关注、拉黑等"""
        if not self.mids or len(self.mids) < 2:
            return

        events = [
            (EventTypes.SOCIAL_FOLLOW, 35),
            (EventTypes.SOCIAL_UNFOLLOW, 15),
            (EventTypes.SOCIAL_BLOCK, 5),
            (EventTypes.SOCIAL_UNBLOCK, 3),
            (EventTypes.SOCIAL_WHISPER, 30),
            (EventTypes.SOCIAL_REPORT_USER, 2),
        ]

        data = []
        for _ in range(count):
            event_type = random.choices([e[0] for e in events], weights=[e[1] for e in events])[0]
            mid = random.choice(self.mids)
            target_mid = random.choice([m for m in self.mids if m != mid])
            base = self._get_base_event_data(mid)

            props = {
                "event_type": event_type,
                "target_mid": target_mid,
                "from": random.choice(["space", "video", "comment", "search", "recommend", "dynamic"])
            }

            if event_type == EventTypes.SOCIAL_FOLLOW:
                props.update({
                    "target_level": random.randint(0, 6),
                    "target_follower_count": random.randint(0, 1000000),
                    "is_mutual": random.choice([True, False]),
                    "special_group": random.choice([None, "特别关注", "悄悄关注"])
                })
            elif event_type == EventTypes.SOCIAL_UNFOLLOW:
                props.update({
                    "follow_duration_days": random.randint(1, 1000),
                    "unfollow_reason": random.choice(["not_interested", "too_many_updates", "content_changed", "manual"])
                })
            elif event_type == EventTypes.SOCIAL_BLOCK:
                props.update({
                    "block_reason": random.choice(["harassment", "spam", "dislike", "other"]),
                    "is_following": random.choice([True, False])
                })
            elif event_type == EventTypes.SOCIAL_WHISPER:
                props.update({
                    "msg_type": random.choice(["text", "image", "emoji", "share_video"]),
                    "msg_length": random.randint(1, 500),
                    "is_first_msg": random.choice([True, False])
                })
            elif event_type == EventTypes.SOCIAL_REPORT_USER:
                props.update({
                    "report_reason": random.choice(MockDataUtil.REPORT_REASONS),
                    "report_evidence": random.choice(["screenshot", "link", "description"])
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "target_mid": target_mid,
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": f"/space/{target_mid}",
                "referer": random.choice(["https://www.bilibili.com/", f"app://space/{target_mid}", f"app://video/BV1xxx"]),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_SOCIAL, data)
        print(f"Generated {count} social events -> topic: {Constant.TBL_EVENT_SOCIAL}")

    # ==========================================
    # 评论相关埋点生成
    # ==========================================
    def gen_comment_events(self, count: int):
        """生成评论相关埋点: 创建、修改、删除、互动"""
        if not self.mids or not self.bvids:
            return

        events = [
            (EventTypes.COMMENT_CREATE, 25),
            (EventTypes.COMMENT_REPLY, 20),
            (EventTypes.COMMENT_UPDATE, 5),
            (EventTypes.COMMENT_DELETE, 5),
            (EventTypes.COMMENT_REPORT, 3),
            (EventTypes.COMMENT_LIKE, 25),
            (EventTypes.COMMENT_UNLIKE, 8),
            (EventTypes.COMMENT_DISLIKE, 6),
            (EventTypes.COMMENT_UNDISLIKE, 3),
        ]

        data = []
        for _ in range(count):
            event_type = random.choices([e[0] for e in events], weights=[e[1] for e in events])[0]
            mid = random.choice(self.mids)
            bvid = random.choice(self.bvids)
            rpid = random.choice(self.rpids) if self.rpids else MockDataUtil.generate_comment_id()
            base = self._get_base_event_data(mid)

            props = {
                "event_type": event_type,
                "oid": bvid,
                "otype": 1
            }

            if event_type == EventTypes.COMMENT_CREATE:
                new_rpid = MockDataUtil.generate_comment_id()
                props.update({
                    "rpid": new_rpid,
                    "content": random.choice(MockDataUtil.COMMENT_TEMPLATES),
                    "content_length": random.randint(1, 500),
                    "has_emoji": random.choice([True, False]),
                    "has_at": random.choice([True, False]),
                    "from": random.choice(["video_page", "dynamic", "article"])
                })
                rpid = new_rpid
            elif event_type == EventTypes.COMMENT_REPLY:
                new_rpid = MockDataUtil.generate_comment_id()
                props.update({
                    "rpid": new_rpid,
                    "root_rpid": rpid,
                    "parent_rpid": rpid,
                    "content": random.choice(MockDataUtil.COMMENT_TEMPLATES),
                    "content_length": random.randint(1, 200),
                    "reply_to_mid": random.choice(self.mids)
                })
                rpid = new_rpid
            elif event_type == EventTypes.COMMENT_UPDATE:
                props.update({
                    "rpid": rpid,
                    "old_content": "原评论内容",
                    "new_content": random.choice(MockDataUtil.COMMENT_TEMPLATES),
                    "edit_reason": random.choice(["typo", "add_content", "remove_content"])
                })
            elif event_type == EventTypes.COMMENT_DELETE:
                props.update({
                    "rpid": rpid,
                    "delete_reason": random.choice(["self_delete", "regret", "mistake"]),
                    "comment_age_hours": random.randint(1, 720)
                })
            elif event_type == EventTypes.COMMENT_REPORT:
                props.update({
                    "rpid": rpid,
                    "report_reason": random.choice(MockDataUtil.REPORT_REASONS),
                    "report_content_preview": "被举报的内容..."
                })
            elif event_type in [EventTypes.COMMENT_LIKE, EventTypes.COMMENT_UNLIKE]:
                props.update({
                    "rpid": rpid,
                    "is_root_comment": random.choice([True, False]),
                    "comment_owner_mid": random.choice(self.mids)
                })
            elif event_type in [EventTypes.COMMENT_DISLIKE, EventTypes.COMMENT_UNDISLIKE]:
                props.update({
                    "rpid": rpid,
                    "is_root_comment": random.choice([True, False])
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "oid": bvid,
                "rpid": rpid,
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": f"/video/{bvid}#reply",
                "referer": f"app://video/{bvid}",
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_COMMENT, data)
        print(f"Generated {count} comment events -> topic: {Constant.TBL_EVENT_COMMENT}")

    # ==========================================
    # VIP购买埋点生成
    # ==========================================
    def gen_vip_events(self, count: int):
        """生成VIP购买相关埋点"""
        if not self.mids:
            return

        events = [
            (EventTypes.VIP_PAGE_VIEW, 30),
            (EventTypes.VIP_SELECT_PLAN, 20),
            (EventTypes.VIP_CREATE_ORDER, 15),
            (EventTypes.VIP_PAY_START, 12),
            (EventTypes.VIP_PAY_SUCCESS, 8),
            (EventTypes.VIP_PAY_FAIL, 3),
            (EventTypes.VIP_PAY_CANCEL, 5),
            (EventTypes.VIP_AUTO_RENEW_ON, 4),
            (EventTypes.VIP_AUTO_RENEW_OFF, 3),
        ]

        data = []
        for _ in range(count):
            event_type = random.choices([e[0] for e in events], weights=[e[1] for e in events])[0]
            mid = random.choice(self.mids)
            order_no = MockDataUtil.generate_order_no() if event_type not in [EventTypes.VIP_PAGE_VIEW, EventTypes.VIP_SELECT_PLAN] else ""
            base = self._get_base_event_data(mid)
            plan = random.choice(MockDataUtil.VIP_PLANS)

            props = {
                "event_type": event_type,
                "current_vip_type": random.choice([0, 1, 2]),
                "current_vip_expire": int(time.time()) + random.randint(-86400*30, 86400*365) if random.random() > 0.5 else 0
            }

            if event_type == EventTypes.VIP_PAGE_VIEW:
                props.update({
                    "from": random.choice(["home_banner", "video_tip", "space", "settings", "push", "search"]),
                    "page_load_ms": random.randint(100, 3000),
                    "is_promotion_period": random.choice([True, False])
                })
            elif event_type == EventTypes.VIP_SELECT_PLAN:
                props.update({
                    "plan_id": plan["id"],
                    "plan_name": plan["name"],
                    "plan_price": plan["price"],
                    "plan_duration_days": plan["duration_days"],
                    "has_coupon": random.choice([True, False])
                })
            elif event_type == EventTypes.VIP_CREATE_ORDER:
                props.update({
                    "order_no": order_no,
                    "plan_id": plan["id"],
                    "plan_name": plan["name"],
                    "original_price": plan["price"],
                    "final_price": int(plan["price"] * random.choice([1.0, 0.9, 0.8, 0.7])),
                    "coupon_id": f"CPN{random.randint(10000, 99999)}" if random.random() > 0.7 else "",
                    "discount_amount": random.randint(0, 500)
                })
            elif event_type == EventTypes.VIP_PAY_START:
                props.update({
                    "order_no": order_no,
                    "pay_method": random.choice(MockDataUtil.PAY_METHODS),
                    "amount": plan["price"]
                })
            elif event_type == EventTypes.VIP_PAY_SUCCESS:
                props.update({
                    "order_no": order_no,
                    "pay_method": random.choice(MockDataUtil.PAY_METHODS),
                    "amount": plan["price"],
                    "pay_duration_sec": random.randint(5, 120),
                    "new_vip_expire": int(time.time()) + plan["duration_days"] * 86400
                })
            elif event_type == EventTypes.VIP_PAY_FAIL:
                props.update({
                    "order_no": order_no,
                    "pay_method": random.choice(MockDataUtil.PAY_METHODS),
                    "error_code": random.choice(["INSUFFICIENT_BALANCE", "NETWORK_ERROR", "PAYMENT_TIMEOUT", "USER_CANCEL", "SYSTEM_ERROR"]),
                    "error_msg": "支付失败",
                    "retry_count": random.randint(0, 3)
                })
            elif event_type == EventTypes.VIP_PAY_CANCEL:
                props.update({
                    "order_no": order_no,
                    "cancel_stage": random.choice(["before_pay", "during_pay", "after_redirect"]),
                    "time_on_pay_page_sec": random.randint(1, 300)
                })
            elif event_type in [EventTypes.VIP_AUTO_RENEW_ON, EventTypes.VIP_AUTO_RENEW_OFF]:
                props.update({
                    "plan_id": plan["id"],
                    "from": random.choice(["vip_page", "settings", "order_complete"]),
                    "previous_auto_renew": event_type == EventTypes.VIP_AUTO_RENEW_OFF
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "order_no": order_no,
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": "/vip/buy",
                "referer": random.choice(["https://www.bilibili.com/", "app://vip", "app://home"]),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_VIP, data)
        print(f"Generated {count} VIP events -> topic: {Constant.TBL_EVENT_VIP}")


# ==========================================
# 5. 主入口
# ==========================================
if __name__ == "__main__":
    try:
        producer = KafkaProducer(
            bootstrap_servers=Constant.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        print("Connected to Kafka.")

        # 生成数据
        gen = ODSGenerator(producer)

        # 基础数据
        gen.gen_users(200)
        gen.gen_videos(100)
        gen.gen_comments(300)

        # 五类埋点数据
        print("\n--- Generating Event Tracking Data ---")
        gen.gen_account_events(500)      # 1. 账号相关 (创建/修改/删除)
        gen.gen_video_events(2000)       # 2. 视频相关 (上传/修改/删除) + 4. 视频互动 (点赞/投币/收藏)
        gen.gen_social_events(800)       # 3. 账号互动 (关注/拉黑)
        gen.gen_comment_events(1000)     # 5. 评论相关 (创建/修改/删除) + 6. 评论互动 (点赞/点踩)
        gen.gen_vip_events(400)          # 7. VIP购买

        producer.close()
        print("\n=== All Done! ===")
        print("Generated topics:")
        print(f"  - {Constant.TBL_USER} (用户数据)")
        print(f"  - {Constant.TBL_VIDEO} (视频数据)")
        print(f"  - {Constant.TBL_COMMENT} (评论数据)")
        print(f"  - {Constant.TBL_EVENT_ACCOUNT} (账号埋点)")
        print(f"  - {Constant.TBL_EVENT_VIDEO} (视频埋点)")
        print(f"  - {Constant.TBL_EVENT_SOCIAL} (社交埋点)")
        print(f"  - {Constant.TBL_EVENT_COMMENT} (评论埋点)")
        print(f"  - {Constant.TBL_EVENT_VIP} (VIP埋点)")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
