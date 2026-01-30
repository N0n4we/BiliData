from kafka import KafkaProducer
import random
import math
import time
import uuid
import json
import socket
import struct
import signal
import sys
from typing import Dict, Any, List


class DistributionUtil:
    @staticmethod
    def pareto(alpha: float = 1.5, x_min: float = 1.0) -> float:
        u = random.random()
        return x_min / (u ** (1.0 / alpha))

    @staticmethod
    def zipf_index(n: int, s: float = 1.2) -> int:
        if n <= 0:
            return 0
        weights = [1.0 / ((i + 1) ** s) for i in range(n)]
        total = sum(weights)
        r = random.random() * total
        cumsum = 0.0
        for i, w in enumerate(weights):
            cumsum += w
            if r <= cumsum:
                return i
        return n - 1

    @staticmethod
    def exponential(lambd: float = 1.0) -> float:
        return random.expovariate(lambd)

    @staticmethod
    def log_normal(mu: float = 0.0, sigma: float = 1.0) -> float:
        return random.lognormvariate(mu, sigma)

    @staticmethod
    def beta_biased(a: float = 2.0, b: float = 5.0) -> float:
        return random.betavariate(a, b)

    @staticmethod
    def triangular_biased(low: float, high: float, mode: float) -> float:
        return random.triangular(low, high, mode)

    @staticmethod
    def biased_bool(true_prob: float = 0.5) -> bool:
        return random.random() < true_prob

    @staticmethod
    def biased_choice(items: list, bias: str = "front") -> Any:
        if not items:
            return None
        n = len(items)
        if n == 1:
            return items[0]

        if bias == "front":
            idx = DistributionUtil.zipf_index(n, s=1.5)
        elif bias == "back":
            idx = n - 1 - DistributionUtil.zipf_index(n, s=1.5)
        else:
            idx = int(random.gauss(n / 2, n / 6))
            idx = max(0, min(n - 1, idx))
        return items[idx]

    @staticmethod
    def skewed_int(low: int, high: int, skew: str = "low") -> int:
        if low >= high:
            return low

        if skew == "low":
            ratio = random.betavariate(1.5, 5)
        elif skew == "high":
            ratio = random.betavariate(5, 1.5)
        else:
            ratio = random.betavariate(4, 4)

        return int(low + ratio * (high - low))

    @staticmethod
    def long_tail_int(low: int, high: int, tail_factor: float = 2.0) -> int:
        if low >= high:
            return low
        range_size = high - low
        val = DistributionUtil.log_normal(0, tail_factor / 3)
        normalized = 1 - math.exp(-val)
        return int(low + normalized * range_size)

    @staticmethod
    def clustered_choice(items: list, cluster_size: int = 3) -> Any:
        if not items:
            return None
        n = len(items)
        if n <= cluster_size:
            return random.choice(items)

        if random.random() < 0.8:
            hot_indices = random.sample(range(n), min(cluster_size, n))
            return items[random.choice(hot_indices)]
        else:
            return random.choice(items)

class Constant:
    KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
    TBL_USER = "app_user_profile"
    TBL_VIDEO = "app_video_content"
    TBL_COMMENT = "app_comment"
    TBL_EVENT_ACCOUNT = "app_event_account"
    TBL_EVENT_VIDEO = "app_event_video"
    TBL_EVENT_SOCIAL = "app_event_social"
    TBL_EVENT_COMMENT = "app_event_comment"
    TBL_EVENT_VIP = "app_event_vip"


class EventTypes:
    ACCOUNT_REGISTER = "account_register"
    ACCOUNT_LOGIN = "account_login"
    ACCOUNT_LOGOUT = "account_logout"
    ACCOUNT_UPDATE_PROFILE = "account_update_profile"
    ACCOUNT_UPDATE_AVATAR = "account_update_avatar"
    ACCOUNT_UPDATE_PASSWORD = "account_update_password"
    ACCOUNT_DEACTIVATE = "account_deactivate"
    ACCOUNT_BIND_PHONE = "account_bind_phone"
    ACCOUNT_BIND_EMAIL = "account_bind_email"
    VIDEO_UPLOAD_START = "video_upload_start"
    VIDEO_UPLOAD_PROGRESS = "video_upload_progress"
    VIDEO_UPLOAD_COMPLETE = "video_upload_complete"
    VIDEO_UPLOAD_FAIL = "video_upload_fail"
    VIDEO_UPDATE_INFO = "video_update_info"
    VIDEO_DELETE = "video_delete"
    VIDEO_SET_PRIVATE = "video_set_private"
    VIDEO_PUBLISH = "video_publish"
    VIDEO_VIEW = "video_view"
    VIDEO_PLAY_HEARTBEAT = "video_play_heartbeat"
    SOCIAL_FOLLOW = "social_follow"
    SOCIAL_UNFOLLOW = "social_unfollow"
    SOCIAL_BLOCK = "social_block"
    SOCIAL_UNBLOCK = "social_unblock"
    SOCIAL_WHISPER = "social_whisper"
    SOCIAL_REPORT_USER = "social_report_user"
    VIDEO_LIKE = "video_like"
    VIDEO_UNLIKE = "video_unlike"
    VIDEO_COIN = "video_coin"
    VIDEO_FAVORITE = "video_favorite"
    VIDEO_UNFAVORITE = "video_unfavorite"
    VIDEO_SHARE = "video_share"
    VIDEO_DANMAKU = "video_danmaku"
    VIDEO_TRIPLE = "video_triple"
    COMMENT_CREATE = "comment_create"
    COMMENT_REPLY = "comment_reply"
    COMMENT_UPDATE = "comment_update"
    COMMENT_DELETE = "comment_delete"
    COMMENT_REPORT = "comment_report"
    COMMENT_LIKE = "comment_like"
    COMMENT_UNLIKE = "comment_unlike"
    COMMENT_DISLIKE = "comment_dislike"
    COMMENT_UNDISLIKE = "comment_undislike"
    VIP_PAGE_VIEW = "vip_page_view"
    VIP_SELECT_PLAN = "vip_select_plan"
    VIP_CREATE_ORDER = "vip_create_order"
    VIP_PAY_START = "vip_pay_start"
    VIP_PAY_SUCCESS = "vip_pay_success"
    VIP_PAY_FAIL = "vip_pay_fail"
    VIP_PAY_CANCEL = "vip_pay_cancel"
    VIP_AUTO_RENEW_ON = "vip_auto_renew_on"
    VIP_AUTO_RENEW_OFF = "vip_auto_renew_off"


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


class RoutineConfig:
    INTERVAL_MIN = 0.3
    INTERVAL_MAX = 2.5
    BATCH_USERS = 2
    BATCH_VIDEOS = 1
    BATCH_COMMENTS = 3
    BATCH_ACCOUNT_EVENTS = 5
    BATCH_VIDEO_EVENTS = 20
    BATCH_SOCIAL_EVENTS = 8
    BATCH_COMMENT_EVENTS = 10
    BATCH_VIP_EVENTS = 4
    MAX_MIDS = 1000
    MAX_BVIDS = 500
    MAX_RPIDS = 2000


class ODSGenerator:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self.mids: List[int] = []
        self.bvids: List[str] = []
        self.rpids: List[int] = []
        self._hot_mids: List[int] = []
        self._hot_bvids: List[str] = []
        self._hot_rpids: List[int] = []
        self._mid_weights: List[float] = []
        self._bvid_weights: List[float] = []

    def _update_hot_pools(self):
        if self.mids:
            hot_count = max(3, len(self.mids) // 10)
            self._hot_mids = random.sample(self.mids, min(hot_count, len(self.mids)))
            self._mid_weights = [1.0 / ((i + 1) ** 1.2) for i in range(len(self.mids))]

        if self.bvids:
            hot_count = max(3, len(self.bvids) // 8)
            self._hot_bvids = random.sample(self.bvids, min(hot_count, len(self.bvids)))
            self._bvid_weights = [1.0 / ((i + 1) ** 1.3) for i in range(len(self.bvids))]

        if self.rpids:
            hot_count = max(5, len(self.rpids) // 15)
            self._hot_rpids = random.sample(self.rpids, min(hot_count, len(self.rpids)))

    def _pick_mid(self, prefer_hot: bool = True) -> int:
        if not self.mids:
            return random.randint(10000000, 99999999)

        if prefer_hot and self._hot_mids and random.random() < 0.7:
            return random.choice(self._hot_mids)
        else:
            idx = DistributionUtil.zipf_index(len(self.mids), s=1.2)
            return self.mids[idx]

    def _pick_bvid(self, prefer_hot: bool = True) -> str:
        if not self.bvids:
            return MockDataUtil.generate_bvid()

        if prefer_hot and self._hot_bvids and random.random() < 0.75:
            return random.choice(self._hot_bvids)
        else:
            idx = DistributionUtil.zipf_index(len(self.bvids), s=1.3)
            return self.bvids[idx]

    def _pick_rpid(self, prefer_hot: bool = True) -> int:
        if not self.rpids:
            return MockDataUtil.generate_comment_id()

        if prefer_hot and self._hot_rpids and random.random() < 0.6:
            return random.choice(self._hot_rpids)
        else:
            idx = DistributionUtil.zipf_index(len(self.rpids), s=1.1)
            return self.rpids[idx]

    def _send_to_kafka(self, topic: str, messages: List[Dict]):
        for msg in messages:
            self.producer.send(topic, value=msg)
        self.producer.flush()

    def _get_base_event_data(self, mid: int):
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

    def _trim_pools(self):
        if len(self.mids) > RoutineConfig.MAX_MIDS:
            self.mids = self.mids[-RoutineConfig.MAX_MIDS:]
        if len(self.bvids) > RoutineConfig.MAX_BVIDS:
            self.bvids = self.bvids[-RoutineConfig.MAX_BVIDS:]
        if len(self.rpids) > RoutineConfig.MAX_RPIDS:
            self.rpids = self.rpids[-RoutineConfig.MAX_RPIDS:]

    def gen_users(self, count: int):
        data = []
        for _ in range(count):
            mid = random.randint(10000000, 99999999)
            self.mids.append(mid)

            verify_type = -1 if random.random() < 0.92 else random.choice([0, 1])
            verify_json = {
                "type": verify_type,
                "desc": "bilibili 知名UP主" if verify_type == 1 else ""
            }

            settings_json = {
                "privacy": {"show_fav": DistributionUtil.biased_bool(0.6), "show_history": DistributionUtil.biased_bool(0.4)},
                "push": {"comment": True, "like": DistributionUtil.biased_bool(0.7), "at": True},
                "theme": DistributionUtil.biased_choice(["auto", "light", "dark"], bias="front")
            }

            tags = random.sample(["宅", "萌", "技术宅", "古风", "鬼畜", "一般路过", "游戏", "动漫", "音乐", "舞蹈"], k=DistributionUtil.skewed_int(0, 4, skew="low"))

            level = DistributionUtil.skewed_int(0, 6, skew="low")
            coins = round(DistributionUtil.long_tail_int(0, 5000, tail_factor=2.5), 1)
            vip_type = 0 if random.random() < 0.85 else random.choice([1, 2])

            data.append({
                "mid": mid,
                "nick_name": MockDataUtil.generate_username(),
                "sex": DistributionUtil.biased_choice(["男", "女", "保密"], bias="front"),
                "face_url": f"https://i0.hdslb.com/bfs/face/{uuid.uuid4().hex}.jpg",
                "sign": DistributionUtil.biased_choice(["这个人很懒", "我的简介？不存在的", "二次元浓度超标", "干杯！", ""], bias="back"),
                "level": level,
                "birthday": f"{DistributionUtil.skewed_int(1985, 2008, skew='high')}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "coins": coins,
                "vip_type": vip_type,
                "official_verify": verify_json,
                "settings": settings_json,
                "tags": tags,
                "status": 0,
                "created_at": int(time.time() * 1000),
                "updated_at": int(time.time() * 1000)
            })

        self._update_hot_pools()
        self._send_to_kafka(Constant.TBL_USER, data)
        self._trim_pools()
        return count

    def gen_videos(self, count: int):
        if not self.mids:
            return 0

        data = []
        titles = ["【原创】", "【转载】", "【自制】", "【搬运】", "【4K】", "【教程】", "【测评】", "【vlog】"]
        topics = ["这个视频太绝了", "年度最佳", "挑战不可能", "一看就会", "新手必看", "深度解析", "独家揭秘", "全网首发"]

        for _ in range(count):
            bvid = MockDataUtil.generate_bvid()
            self.bvids.append(bvid)
            mid = self._pick_mid(prefer_hot=True)
            category = DistributionUtil.biased_choice(MockDataUtil.VIDEO_CATEGORIES, bias="front")

            meta = {
                "upload_ip": MockDataUtil.generate_ip(),
                "camera": DistributionUtil.biased_choice(["Sony A7M4", "iPhone 15 Pro", "Canon R5", "GoPro 12", "DJI Pocket 3"], bias="front"),
                "software": DistributionUtil.biased_choice(["Premiere Pro", "Final Cut Pro", "DaVinci Resolve", "剪映"], bias="back"),
                "resolution": DistributionUtil.biased_choice(["1080p", "2k", "4k"], bias="front"),
                "fps": DistributionUtil.biased_choice([24, 30, 60, 120], bias="front")
            }

            stats = {
                "view": DistributionUtil.long_tail_int(0, 1000000, tail_factor=3.0),
                "danmaku": DistributionUtil.long_tail_int(0, 10000, tail_factor=2.5),
                "reply": DistributionUtil.long_tail_int(0, 5000, tail_factor=2.5),
                "favorite": DistributionUtil.long_tail_int(0, 20000, tail_factor=2.8),
                "coin": DistributionUtil.long_tail_int(0, 30000, tail_factor=2.8),
                "share": DistributionUtil.long_tail_int(0, 5000, tail_factor=2.5),
                "like": DistributionUtil.long_tail_int(0, 50000, tail_factor=3.0)
            }

            audit = [
                {"ts": int(time.time()) - DistributionUtil.skewed_int(0, 86400, skew="low"), "operator": "system", "status": "pass"},
            ]

            pubdate = int(time.time() * 1000) - DistributionUtil.long_tail_int(0, 365, tail_factor=2.0) * 86400 * 1000

            data.append({
                "bvid": bvid,
                "title": f"{DistributionUtil.biased_choice(titles, bias='front')}{random.choice(topics)}_{bvid[-6:]}",
                "cover": f"https://i0.hdslb.com/bfs/archive/{uuid.uuid4().hex}.jpg",
                "desc_text": "视频简介...",
                "duration": DistributionUtil.skewed_int(30, 7200, skew="low"),
                "pubdate": pubdate,
                "mid": mid,
                "category_id": random.randint(1, 200),
                "category_name": category,
                "state": 0,
                "attribute": 0,
                "is_private": DistributionUtil.biased_bool(0.05),
                "meta_info": meta,
                "stats": stats,
                "audit_info": audit,
                "created_at": int(time.time() * 1000),
                "updated_at": int(time.time() * 1000)
            })

        self._update_hot_pools()
        self._send_to_kafka(Constant.TBL_VIDEO, data)
        self._trim_pools()
        return count

    def gen_comments(self, count: int):
        if not self.mids or not self.bvids:
            return 0

        data = []
        for _ in range(count):
            rpid = MockDataUtil.generate_comment_id()
            self.rpids.append(rpid)

            is_reply = DistributionUtil.biased_bool(0.25) and len(self.rpids) > 10
            root = self._pick_rpid(prefer_hot=True) if is_reply else 0

            created_at = int(time.time() * 1000) - DistributionUtil.long_tail_int(0, 720, tail_factor=2.0) * 3600 * 1000

            like_count = DistributionUtil.long_tail_int(0, 1000, tail_factor=2.5)
            reply_count = DistributionUtil.long_tail_int(0, 50, tail_factor=2.0) if not is_reply else 0

            data.append({
                "rpid": rpid,
                "oid": self._pick_bvid(prefer_hot=True),
                "otype": 1,
                "mid": self._pick_mid(prefer_hot=True),
                "root": root,
                "parent": root,
                "content": DistributionUtil.biased_choice(MockDataUtil.COMMENT_TEMPLATES, bias="front"),
                "like_count": like_count,
                "dislike_count": 0,
                "reply_count": reply_count,
                "state": 0,
                "created_at": created_at,
                "updated_at": created_at
            })

        self._update_hot_pools()
        self._send_to_kafka(Constant.TBL_COMMENT, data)
        self._trim_pools()
        return count

    def gen_account_events(self, count: int):
        if not self.mids:
            return 0

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
            mid = self._pick_mid(prefer_hot=True) if event_type != EventTypes.ACCOUNT_REGISTER else random.randint(90000000, 99999999)
            base = self._get_base_event_data(mid)

            props = {"event_type": event_type, "source_page": DistributionUtil.biased_choice(["settings", "profile", "login", "app_start"], bias="front")}

            if event_type == EventTypes.ACCOUNT_REGISTER:
                props.update({
                    "register_type": DistributionUtil.biased_choice(["phone", "email", "qq", "wechat", "weibo"], bias="front"),
                    "invite_code": f"INV{random.randint(10000, 99999)}" if DistributionUtil.biased_bool(0.15) else "",
                    "channel": DistributionUtil.biased_choice(MockDataUtil.CHANNELS, bias="front")
                })
            elif event_type == EventTypes.ACCOUNT_LOGIN:
                props.update({
                    "login_type": DistributionUtil.biased_choice(["password", "sms", "qrcode", "fingerprint", "face_id"], bias="front"),
                    "is_auto_login": DistributionUtil.biased_bool(0.6),
                    "login_result": "success" if DistributionUtil.biased_bool(0.96) else "fail"
                })
            elif event_type == EventTypes.ACCOUNT_UPDATE_PROFILE:
                props.update({
                    "updated_fields": random.sample(["nick_name", "sign", "sex", "birthday"], k=DistributionUtil.skewed_int(1, 3, skew="low")),
                    "before_snapshot": {"nick_name": "旧昵称"},
                    "after_snapshot": {"nick_name": "新昵称"}
                })
            elif event_type == EventTypes.ACCOUNT_UPDATE_AVATAR:
                props.update({
                    "avatar_source": DistributionUtil.biased_choice(["camera", "album", "system_preset"], bias="middle"),
                    "file_size_kb": DistributionUtil.skewed_int(100, 5000, skew="low")
                })
            elif event_type == EventTypes.ACCOUNT_DEACTIVATE:
                props.update({
                    "reason": DistributionUtil.biased_choice(["不再使用", "隐私顾虑", "账号安全", "其他"], bias="front"),
                    "account_age_days": DistributionUtil.long_tail_int(30, 2000, tail_factor=2.0)
                })
            elif event_type in [EventTypes.ACCOUNT_BIND_PHONE, EventTypes.ACCOUNT_BIND_EMAIL]:
                props.update({
                    "bind_result": "success" if DistributionUtil.biased_bool(0.92) else "fail",
                    "is_rebind": DistributionUtil.biased_bool(0.2)
                })

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": f"/account/{event_type.split('_')[-1]}",
                "referer": DistributionUtil.biased_choice(["https://www.bilibili.com/", "app://settings", "app://profile"], bias="front"),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_ACCOUNT, data)
        return count

    def gen_video_events(self, count: int):
        if not self.mids:
            return 0

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
            mid = self._pick_mid(prefer_hot=True)
            bvid = self._pick_bvid(prefer_hot=True) if self.bvids else MockDataUtil.generate_bvid()
            base = self._get_base_event_data(mid)

            props = {"event_type": event_type}

            if event_type == EventTypes.VIDEO_VIEW:
                props.update({
                    "bvid": bvid,
                    "from": DistributionUtil.biased_choice(MockDataUtil.PAGE_SOURCES, bias="front"),
                    "spm_id": f"{random.randint(100,999)}.{random.randint(100,999)}.0.0",
                    "is_auto_play": DistributionUtil.biased_bool(0.35),
                    "quality": DistributionUtil.biased_choice(["360p", "480p", "720p", "1080p", "4k"], bias="middle")
                })
            elif event_type == EventTypes.VIDEO_PLAY_HEARTBEAT:
                total_sec = DistributionUtil.skewed_int(60, 7200, skew="low")
                props.update({
                    "bvid": bvid,
                    "progress_sec": DistributionUtil.skewed_int(1, total_sec, skew="low"),
                    "total_sec": total_sec,
                    "play_speed": DistributionUtil.biased_choice([0.5, 1.0, 1.25, 1.5, 2.0], bias="middle"),
                    "quality": DistributionUtil.biased_choice(["720p", "1080p", "4k"], bias="front"),
                    "is_full_screen": DistributionUtil.biased_bool(0.4)
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_START:
                props.update({
                    "file_size_mb": DistributionUtil.long_tail_int(10, 5000, tail_factor=2.5),
                    "duration_sec": DistributionUtil.skewed_int(30, 7200, skew="low"),
                    "resolution": DistributionUtil.biased_choice(["1080p", "2k", "4k"], bias="front"),
                    "upload_type": "single" if DistributionUtil.biased_bool(0.7) else "multi_part"
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_PROGRESS:
                props.update({
                    "upload_id": str(uuid.uuid4()),
                    "progress_percent": DistributionUtil.skewed_int(1, 99, skew="middle"),
                    "uploaded_mb": DistributionUtil.skewed_int(1, 1000, skew="low"),
                    "speed_kbps": DistributionUtil.long_tail_int(500, 50000, tail_factor=2.0)
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_COMPLETE:
                props.update({
                    "bvid": bvid,
                    "title": f"新上传视频_{bvid[-6:]}",
                    "category": DistributionUtil.biased_choice(MockDataUtil.VIDEO_CATEGORIES, bias="front"),
                    "duration_sec": DistributionUtil.skewed_int(30, 7200, skew="low"),
                    "upload_duration_sec": DistributionUtil.long_tail_int(10, 600, tail_factor=2.0)
                })
            elif event_type == EventTypes.VIDEO_UPLOAD_FAIL:
                props.update({
                    "error_code": DistributionUtil.biased_choice(["NETWORK_ERROR", "FILE_TOO_LARGE", "FORMAT_NOT_SUPPORT", "SERVER_ERROR"], bias="front"),
                    "error_msg": "上传失败",
                    "retry_count": DistributionUtil.skewed_int(0, 3, skew="low")
                })
            elif event_type == EventTypes.VIDEO_UPDATE_INFO:
                props.update({
                    "bvid": bvid,
                    "updated_fields": random.sample(["title", "desc", "cover", "tags", "category"], k=DistributionUtil.skewed_int(1, 3, skew="low"))
                })
            elif event_type == EventTypes.VIDEO_DELETE:
                props.update({
                    "bvid": bvid,
                    "delete_reason": DistributionUtil.biased_choice(["personal", "copyright", "mistake", "other"], bias="front"),
                    "video_age_days": DistributionUtil.long_tail_int(1, 365, tail_factor=2.0)
                })
            elif event_type in [EventTypes.VIDEO_LIKE, EventTypes.VIDEO_UNLIKE]:
                props.update({
                    "bvid": bvid,
                    "from": DistributionUtil.biased_choice(MockDataUtil.PAGE_SOURCES, bias="front"),
                    "is_liked_before": event_type == EventTypes.VIDEO_UNLIKE
                })
            elif event_type == EventTypes.VIDEO_COIN:
                props.update({
                    "bvid": bvid,
                    "coin_count": 2 if DistributionUtil.biased_bool(0.6) else 1,
                    "with_like": DistributionUtil.biased_bool(0.7),
                    "remain_coins": DistributionUtil.long_tail_int(0, 500, tail_factor=2.0)
                })
            elif event_type in [EventTypes.VIDEO_FAVORITE, EventTypes.VIDEO_UNFAVORITE]:
                props.update({
                    "bvid": bvid,
                    "fav_folder_ids": [random.randint(1, 100) for _ in range(DistributionUtil.skewed_int(1, 3, skew="low"))],
                    "create_new_folder": DistributionUtil.biased_bool(0.1) if event_type == EventTypes.VIDEO_FAVORITE else False
                })
            elif event_type == EventTypes.VIDEO_SHARE:
                props.update({
                    "bvid": bvid,
                    "share_channel": DistributionUtil.biased_choice(MockDataUtil.SHARE_CHANNELS, bias="front"),
                    "share_result": DistributionUtil.biased_choice(["success", "cancel", "fail"], bias="front")
                })
            elif event_type == EventTypes.VIDEO_DANMAKU:
                props.update({
                    "bvid": bvid,
                    "content": DistributionUtil.biased_choice(["哈哈哈", "awsl", "前方高能", "泪目", "太强了"], bias="front"),
                    "progress_sec": DistributionUtil.skewed_int(1, 600, skew="low"),
                    "danmaku_type": DistributionUtil.biased_choice(["scroll", "top", "bottom"], bias="front"),
                    "color": DistributionUtil.biased_choice(["#FFFFFF", "#FE0302", "#FFFF00"], bias="front")
                })
            elif event_type == EventTypes.VIDEO_TRIPLE:
                props.update({
                    "bvid": bvid,
                    "from": DistributionUtil.biased_choice(MockDataUtil.PAGE_SOURCES, bias="front"),
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
                "referer": DistributionUtil.biased_choice(["https://www.bilibili.com/", f"app://video/{bvid}", "app://home"], bias="front"),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_VIDEO, data)
        return count

    def gen_social_events(self, count: int):
        if not self.mids or len(self.mids) < 2:
            return 0

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
            mid = self._pick_mid(prefer_hot=True)
            target_mid = self._pick_mid(prefer_hot=True)
            while target_mid == mid:
                target_mid = self._pick_mid(prefer_hot=False)
            base = self._get_base_event_data(mid)

            props = {
                "event_type": event_type,
                "target_mid": target_mid,
                "from": DistributionUtil.biased_choice(["space", "video", "comment", "search", "recommend", "dynamic"], bias="front")
            }

            if event_type == EventTypes.SOCIAL_FOLLOW:
                props.update({
                    "target_level": DistributionUtil.skewed_int(0, 6, skew="high"),
                    "target_follower_count": DistributionUtil.long_tail_int(0, 1000000, tail_factor=3.0),
                    "is_mutual": DistributionUtil.biased_bool(0.15),
                    "special_group": DistributionUtil.biased_choice([None, None, None, "特别关注", "悄悄关注"], bias="front")
                })
            elif event_type == EventTypes.SOCIAL_UNFOLLOW:
                props.update({
                    "follow_duration_days": DistributionUtil.long_tail_int(1, 1000, tail_factor=2.0),
                    "unfollow_reason": DistributionUtil.biased_choice(["not_interested", "too_many_updates", "content_changed", "manual"], bias="front")
                })
            elif event_type == EventTypes.SOCIAL_BLOCK:
                props.update({
                    "block_reason": DistributionUtil.biased_choice(["harassment", "spam", "dislike", "other"], bias="front"),
                    "is_following": DistributionUtil.biased_bool(0.1)
                })
            elif event_type == EventTypes.SOCIAL_WHISPER:
                props.update({
                    "msg_type": DistributionUtil.biased_choice(["text", "image", "emoji", "share_video"], bias="front"),
                    "msg_length": DistributionUtil.skewed_int(1, 500, skew="low"),
                    "is_first_msg": DistributionUtil.biased_bool(0.2)
                })
            elif event_type == EventTypes.SOCIAL_REPORT_USER:
                props.update({
                    "report_reason": DistributionUtil.biased_choice(MockDataUtil.REPORT_REASONS, bias="front"),
                    "report_evidence": DistributionUtil.biased_choice(["screenshot", "link", "description"], bias="front")
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
                "referer": DistributionUtil.biased_choice(["https://www.bilibili.com/", f"app://space/{target_mid}", f"app://video/BV1xxx"], bias="front"),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_SOCIAL, data)
        return count

    def gen_comment_events(self, count: int):
        if not self.mids or not self.bvids:
            return 0

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
            mid = self._pick_mid(prefer_hot=True)
            bvid = self._pick_bvid(prefer_hot=True)
            rpid = self._pick_rpid(prefer_hot=True) if self.rpids else MockDataUtil.generate_comment_id()
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
                    "content": DistributionUtil.biased_choice(MockDataUtil.COMMENT_TEMPLATES, bias="front"),
                    "content_length": DistributionUtil.skewed_int(1, 500, skew="low"),
                    "has_emoji": DistributionUtil.biased_bool(0.35),
                    "has_at": DistributionUtil.biased_bool(0.15),
                    "from": DistributionUtil.biased_choice(["video_page", "dynamic", "article"], bias="front")
                })
                rpid = new_rpid
            elif event_type == EventTypes.COMMENT_REPLY:
                new_rpid = MockDataUtil.generate_comment_id()
                props.update({
                    "rpid": new_rpid,
                    "root_rpid": rpid,
                    "parent_rpid": rpid,
                    "content": DistributionUtil.biased_choice(MockDataUtil.COMMENT_TEMPLATES, bias="front"),
                    "content_length": DistributionUtil.skewed_int(1, 200, skew="low"),
                    "reply_to_mid": self._pick_mid(prefer_hot=True)
                })
                rpid = new_rpid
            elif event_type == EventTypes.COMMENT_UPDATE:
                props.update({
                    "rpid": rpid,
                    "old_content": "原评论内容",
                    "new_content": DistributionUtil.biased_choice(MockDataUtil.COMMENT_TEMPLATES, bias="front"),
                    "edit_reason": DistributionUtil.biased_choice(["typo", "add_content", "remove_content"], bias="front")
                })
            elif event_type == EventTypes.COMMENT_DELETE:
                props.update({
                    "rpid": rpid,
                    "delete_reason": DistributionUtil.biased_choice(["self_delete", "regret", "mistake"], bias="front"),
                    "comment_age_hours": DistributionUtil.long_tail_int(1, 720, tail_factor=2.0)
                })
            elif event_type == EventTypes.COMMENT_REPORT:
                props.update({
                    "rpid": rpid,
                    "report_reason": DistributionUtil.biased_choice(MockDataUtil.REPORT_REASONS, bias="front"),
                    "report_content_preview": "被举报的内容..."
                })
            elif event_type in [EventTypes.COMMENT_LIKE, EventTypes.COMMENT_UNLIKE]:
                props.update({
                    "rpid": rpid,
                    "is_root_comment": DistributionUtil.biased_bool(0.7),
                    "comment_owner_mid": self._pick_mid(prefer_hot=True)
                })
            elif event_type in [EventTypes.COMMENT_DISLIKE, EventTypes.COMMENT_UNDISLIKE]:
                props.update({
                    "rpid": rpid,
                    "is_root_comment": DistributionUtil.biased_bool(0.7)
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
        return count

    def gen_vip_events(self, count: int):
        if not self.mids:
            return 0

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
            mid = self._pick_mid(prefer_hot=True)
            base = self._get_base_event_data(mid)
            plan = DistributionUtil.biased_choice(MockDataUtil.VIP_PLANS, bias="front")
            pay_method = DistributionUtil.biased_choice(MockDataUtil.PAY_METHODS, bias="front")
            order_no = MockDataUtil.generate_order_no() if event_type not in [EventTypes.VIP_PAGE_VIEW, EventTypes.VIP_SELECT_PLAN] else ""
            has_coupon = DistributionUtil.biased_bool(0.25)
            coupon_id = f"CPN{random.randint(10000, 99999)}" if has_coupon else ""
            discount_amount = DistributionUtil.skewed_int(100, 500, skew="low") if has_coupon else 0
            final_price = plan["price"] - discount_amount

            props = {
                "event_type": event_type,
                "current_vip_type": 0 if DistributionUtil.biased_bool(0.7) else random.choice([1, 2]),
                "current_vip_expire": int(time.time()) + DistributionUtil.skewed_int(-86400*30, 86400*365, skew="low") if DistributionUtil.biased_bool(0.4) else 0,
                "from": DistributionUtil.biased_choice(["home_banner", "video_tip", "space", "settings", "push", "search", "vip_page", "order_complete"], bias="front"),
                "page_load_ms": DistributionUtil.skewed_int(100, 3000, skew="low"),
                "is_promotion_period": DistributionUtil.biased_bool(0.3),
                "plan_id": plan["id"],
                "plan_name": plan["name"],
                "plan_price": plan["price"],
                "plan_duration_days": plan["duration_days"],
                "has_coupon": has_coupon,
                "order_no": order_no,
                "original_price": plan["price"],
                "final_price": final_price,
                "coupon_id": coupon_id,
                "discount_amount": discount_amount,
                "pay_method": pay_method,
                "amount": final_price,
                "pay_duration_sec": DistributionUtil.skewed_int(5, 120, skew="low"),
                "new_vip_expire": int(time.time()) + plan["duration_days"] * 86400,
                "error_code": "",
                "error_msg": "",
                "retry_count": 0,
                "cancel_stage": "",
                "time_on_pay_page_sec": DistributionUtil.skewed_int(1, 300, skew="low"),
                "previous_auto_renew": False
            }

            if event_type == EventTypes.VIP_PAY_FAIL:
                props["error_code"] = DistributionUtil.biased_choice(["INSUFFICIENT_BALANCE", "NETWORK_ERROR", "PAYMENT_TIMEOUT", "USER_CANCEL", "SYSTEM_ERROR"], bias="front")
                props["error_msg"] = "支付失败"
                props["retry_count"] = DistributionUtil.skewed_int(0, 3, skew="low")
            elif event_type == EventTypes.VIP_PAY_CANCEL:
                props["cancel_stage"] = DistributionUtil.biased_choice(["before_pay", "during_pay", "after_redirect"], bias="front")
            elif event_type in [EventTypes.VIP_AUTO_RENEW_ON, EventTypes.VIP_AUTO_RENEW_OFF]:
                props["previous_auto_renew"] = event_type == EventTypes.VIP_AUTO_RENEW_OFF

            data.append({
                "event_id": event_type,
                "trace_id": base["trace_id"],
                "session_id": base["session_id"],
                "mid": base["mid"],
                "order_no": order_no,
                "client_ts": base["client_ts"],
                "server_ts": base["server_ts"],
                "url_path": "/vip/buy",
                "referer": DistributionUtil.biased_choice(["https://www.bilibili.com/", "app://vip", "app://home"], bias="front"),
                "ua": base["ua"],
                "ip": base["ip"],
                "device_info": base["device_info"],
                "app_context": base["app_context"],
                "properties": props
            })

        self._send_to_kafka(Constant.TBL_EVENT_VIP, data)
        return count


class MockRoutine:
    def __init__(self):
        self.running = True
        self.producer = None
        self.generator = None
        self.total_sent = {
            "users": 0,
            "videos": 0,
            "comments": 0,
            "account_events": 0,
            "video_events": 0,
            "social_events": 0,
            "comment_events": 0,
            "vip_events": 0
        }

    def signal_handler(self, signum, frame):
        print("\n\nReceived stop signal. Shutting down gracefully...")
        self.running = False

    def print_stats(self, round_num: int, round_stats: dict):
        print(f"\n[Round {round_num}] Sent this round:")
        print(f"  Users: {round_stats['users']}, Videos: {round_stats['videos']}, Comments: {round_stats['comments']}")
        print(f"  Account Events: {round_stats['account_events']}, Video Events: {round_stats['video_events']}")
        print(f"  Social Events: {round_stats['social_events']}, Comment Events: {round_stats['comment_events']}")
        print(f"  VIP Events: {round_stats['vip_events']}")
        print(f"  Data pool: {len(self.generator.mids)} mids, {len(self.generator.bvids)} bvids, {len(self.generator.rpids)} rpids")

    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=Constant.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            print("Connected to Kafka.")
            print(f"Interval: {RoutineConfig.INTERVAL_MIN}s ~ {RoutineConfig.INTERVAL_MAX}s (randomized)")
            print("Press Ctrl+C to stop.\n")

            self.generator = ODSGenerator(self.producer)

            print("Initializing base data...")
            self.generator.gen_users(50)
            self.generator.gen_videos(20)
            self.generator.gen_comments(50)
            print("Base data initialized.\n")

            round_num = 0
            while self.running:
                round_num += 1
                round_stats = {}

                round_stats["users"] = self.generator.gen_users(RoutineConfig.BATCH_USERS)
                round_stats["videos"] = self.generator.gen_videos(RoutineConfig.BATCH_VIDEOS)
                round_stats["comments"] = self.generator.gen_comments(RoutineConfig.BATCH_COMMENTS)

                round_stats["account_events"] = self.generator.gen_account_events(RoutineConfig.BATCH_ACCOUNT_EVENTS)
                round_stats["video_events"] = self.generator.gen_video_events(RoutineConfig.BATCH_VIDEO_EVENTS)
                round_stats["social_events"] = self.generator.gen_social_events(RoutineConfig.BATCH_SOCIAL_EVENTS)
                round_stats["comment_events"] = self.generator.gen_comment_events(RoutineConfig.BATCH_COMMENT_EVENTS)
                round_stats["vip_events"] = self.generator.gen_vip_events(RoutineConfig.BATCH_VIP_EVENTS)

                for key in round_stats:
                    self.total_sent[key] += round_stats[key]

                self.print_stats(round_num, round_stats)

                if self.running:
                    # 偏向 INTERVAL_MAX，偶尔突发到 INTERVAL_MIN
                    ratio = random.betavariate(5, 1.5)
                    interval = RoutineConfig.INTERVAL_MIN + ratio * (RoutineConfig.INTERVAL_MAX - RoutineConfig.INTERVAL_MIN)
                    time.sleep(interval)

        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print("\n" + "=" * 50)
            print("Total messages sent:")
            for key, value in self.total_sent.items():
                print(f"  {key}: {value}")
            print("=" * 50)

            if self.producer:
                self.producer.close()
                print("\nKafka connection closed.")


if __name__ == "__main__":
    print("=" * 50)
    print("Mock Data Routine - Continuous Kafka Producer")
    print("=" * 50)
    print(f"\nTopics:")
    print(f"  - {Constant.TBL_USER} (用户数据)")
    print(f"  - {Constant.TBL_VIDEO} (视频数据)")
    print(f"  - {Constant.TBL_COMMENT} (评论数据)")
    print(f"  - {Constant.TBL_EVENT_ACCOUNT} (账号埋点)")
    print(f"  - {Constant.TBL_EVENT_VIDEO} (视频埋点)")
    print(f"  - {Constant.TBL_EVENT_SOCIAL} (社交埋点)")
    print(f"  - {Constant.TBL_EVENT_COMMENT} (评论埋点)")
    print(f"  - {Constant.TBL_EVENT_VIP} (VIP埋点)")
    print()

    routine = MockRoutine()
    routine.run()
