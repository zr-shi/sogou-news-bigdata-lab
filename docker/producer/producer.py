import os
import csv
import random
import re
from io import StringIO
import time
from datetime import datetime
from pathlib import Path

import pymysql
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sougoulogs")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5"))
SOURCE_FILE = Path(os.getenv("SOURCE_FILE", "/data/sougou.log"))
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "news")
DB_USER = os.getenv("DB_USER", "hive")
DB_PASSWORD = os.getenv("DB_PASSWORD", "bigdata123")

DYNAMIC_TITLES = os.getenv("PRODUCER_DYNAMIC_TITLES", "true").strip().lower() not in {"0", "false", "no", "off"}
UNIQUE_EVERY = max(1, int(os.getenv("PRODUCER_UNIQUE_EVERY", "5")))
TOPIC_POOL_SIZE = int(os.getenv("PRODUCER_TOPIC_POOL_SIZE", "0"))
INCLUDE_RUN_LABEL = os.getenv("PRODUCER_INCLUDE_RUN_LABEL", "true").strip().lower() not in {"0", "false", "no", "off"}
REALISTIC_TITLES = os.getenv("PRODUCER_REALISTIC_TITLES", "true").strip().lower() not in {"0", "false", "no", "off"}
NOISY_LOG_FORMATS = os.getenv("PRODUCER_NOISY_LOG_FORMATS", "true").strip().lower() not in {"0", "false", "no", "off"}
RUN_LABEL = os.getenv("PRODUCER_RUN_LABEL") or datetime.now().strftime("%Y%m%d%H%M%S")
CONTROL_ENABLED = os.getenv("PRODUCER_CONTROL_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
START_ENABLED = os.getenv("PRODUCER_START_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
CONTROL_POLL_SECONDS = max(1.0, float(os.getenv("PRODUCER_CONTROL_POLL_SECONDS", "2")))
CONTROL_TABLE = os.getenv("PRODUCER_CONTROL_TABLE", "producer_control")
RESET_CONTROL_ON_START = os.getenv("PRODUCER_RESET_CONTROL_ON_START", "true").strip().lower() not in {"0", "false", "no", "off"}
CONTROL_KEY = "generation_enabled"
_last_control_poll = 0.0
_cached_generation_enabled = START_ENABLED

BASE_TOPICS = [
    "人工智能",
    "新能源车",
    "大学生就业",
    "低空经济",
    "体育赛事",
    "电影票房",
    "网络安全",
    "大数据分析",
    "智慧医疗",
    "旅游消费",
    "教育培训",
    "AIGC应用",
    "机器人产业",
    "国产芯片",
    "跨境电商",
    "数字乡村",
    "城市更新",
    "智能制造",
    "绿色金融",
    "文化出海",
]

NEWS_TITLE_PATTERNS = [
    "{city}发布{topic}新政策：{group}关注这些变化",
    "{topic}进入落地关键期：多地试点释放新信号",
    "{company}回应{topic}进展：将加快{action}",
    "{city}{scene}{topic}项目启动：预计带动{effect}",
    "{topic}热度持续攀升：专家提醒关注{risk}",
    "{group}热议{topic}：相关搜索量明显上涨",
    "{topic}迎来新一轮调整：{industry}企业加速布局",
    "{city}推进{topic}应用：首批示范场景公布",
    "{topic}报告发布：{effect}成为年度关键词",
    "{company}与{city}合作建设{topic}平台",
    "{topic}带动消费回暖：{scene}成为热门选择",
    "{industry}观察：{topic}正在改变{group}体验",
    "{topic}赛道融资升温：头部企业抢占{scene}",
    "{city}上线{topic}服务：办事效率进一步提升",
    "{topic}相关岗位需求增长：{group}迎来新机会",
    "{company}发布{topic}解决方案：主打{effect}",
    "{topic}监管细则征求意见：行业进入规范期",
    "{city}{scene}人气回升：{topic}成为讨论焦点",
    "{topic}技术路线再更新：{industry}成本有望下降",
    "{group}怎么看{topic}？调查显示关注点集中在{risk}",
]

CITIES = [
    "北京",
    "上海",
    "深圳",
    "杭州",
    "成都",
    "武汉",
    "西安",
    "广州",
    "苏州",
    "重庆",
]

COMPANIES = [
    "华为",
    "比亚迪",
    "小米",
    "阿里云",
    "腾讯",
    "宁德时代",
    "京东",
    "科大讯飞",
    "美团",
    "百度",
]

GROUPS = [
    "年轻人",
    "中小企业",
    "高校毕业生",
    "消费者",
    "投资者",
    "家长",
    "游客",
    "开发者",
    "制造企业",
    "基层社区",
]

SCENES = [
    "商圈",
    "园区",
    "校园",
    "医院",
    "景区",
    "港口",
    "社区",
    "工厂",
    "展会",
    "交通枢纽",
]

INDUSTRIES = [
    "汽车",
    "文旅",
    "教育",
    "医疗",
    "金融",
    "制造",
    "零售",
    "物流",
    "能源",
    "互联网",
]

ACTIONS = [
    "产品迭代",
    "场景开放",
    "供应链建设",
    "生态合作",
    "人才培养",
    "数据治理",
    "安全评估",
    "渠道下沉",
]

EFFECTS = [
    "降本增效",
    "服务升级",
    "绿色转型",
    "智能化改造",
    "就业扩容",
    "消费复苏",
    "效率提升",
    "产业协同",
]

RISKS = [
    "隐私保护",
    "价格波动",
    "安全边界",
    "人才缺口",
    "数据质量",
    "合规要求",
    "体验落差",
    "供应稳定",
]


def normalize_line(line: str):
    text = line.strip().lstrip("\ufeff")
    if not text:
        return None

    fields = parse_csv_fields(text)
    if len(fields) != 6:
        fields = parse_csv_fields(re.sub(r"\s*(?:\|\||\|)\s*", ",", text))
    if len(fields) != 6:
        fields = parse_csv_fields(re.sub(r"\s*[，、]\s*", ",", text))
    if len(fields) != 6:
        fields = re.split(r"\s+", text, maxsplit=5)
    fields = [field.strip().strip('"').strip("'") for field in fields]
    if len(fields) != 6:
        return None
    fields[0] = normalize_time(fields[0])
    if not fields[0] or not fields[2]:
        return None
    return ",".join(csv_escape(field) for field in fields)


def parse_csv_fields(text: str):
    try:
        return next(csv.reader(StringIO(text), skipinitialspace=True))
    except csv.Error:
        return []


def csv_escape(value: str) -> str:
    if any(char in value for char in [",", '"', "\n", "\r"]):
        return '"' + value.replace('"', '""') + '"'
    return value


def normalize_time(value: str) -> str:
    value = value.strip()
    match = re.search(r"(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?", value)
    if not match:
        return datetime.now().strftime("%H:%M:%S")
    hour = int(match.group(1)) % 24
    minute = int(match.group(2)) % 60
    second = int(match.group(3) or 0) % 60
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def generated_title(index: int) -> str:
    if not DYNAMIC_TITLES:
        return random.choice(BASE_TOPICS[:10])

    if REALISTIC_TITLES:
        topic_index = index // UNIQUE_EVERY
        if TOPIC_POOL_SIZE > 0:
            topic_index = topic_index % TOPIC_POOL_SIZE
        template = NEWS_TITLE_PATTERNS[topic_index % len(NEWS_TITLE_PATTERNS)]
        topic = BASE_TOPICS[topic_index % len(BASE_TOPICS)]
        return template.format(
            city=CITIES[(topic_index // 2) % len(CITIES)],
            company=COMPANIES[(topic_index // 3) % len(COMPANIES)],
            group=GROUPS[(topic_index // 5) % len(GROUPS)],
            scene=SCENES[(topic_index // 7) % len(SCENES)],
            industry=INDUSTRIES[(topic_index // 11) % len(INDUSTRIES)],
            action=ACTIONS[(topic_index // 13) % len(ACTIONS)],
            effect=EFFECTS[(topic_index // 17) % len(EFFECTS)],
            risk=RISKS[(topic_index // 19) % len(RISKS)],
            topic=topic,
        )

    bucket = index // UNIQUE_EVERY
    topic_index = bucket if TOPIC_POOL_SIZE <= 0 else bucket % TOPIC_POOL_SIZE
    base = BASE_TOPICS[topic_index % len(BASE_TOPICS)]
    suffix = ["最新进展", "行业观察", "政策解读", "用户热议", "市场表现"][topic_index % 5]
    serial = f"{topic_index + 1:04d}"
    if INCLUDE_RUN_LABEL:
        serial = f"{RUN_LABEL}-{serial}"
    return f"{base}-{suffix}-{serial}"


def generated_line(index: int) -> str:
    now = datetime.now()
    fields = [
        now.strftime("%H:%M:%S"),
        str(random.randint(100000, 999999)),
        generated_title(index),
        str(random.randint(1, 50)),
        str(random.randint(1, 20)),
        random.choice(["web", "app", "search"]),
    ]
    if not NOISY_LOG_FORMATS:
        return ",".join(csv_escape(field) for field in fields)

    variant = index % 8
    if variant == 1:
        return " , ".join(csv_escape(field) for field in fields)
    if variant == 2:
        return "|".join(fields)
    if variant == 3:
        return "||".join(fields)
    if variant == 4:
        return "\t".join(fields)
    if variant == 5:
        return "，".join(fields)
    if variant == 6:
        noisy = fields[:]
        noisy[2] = f"“{noisy[2]}”"
        return ",".join(csv_escape(field) for field in noisy)
    return ",".join(csv_escape(field) for field in fields)


def source_lines():
    if not SOURCE_FILE.exists():
        return []
    for encoding in ("utf-8", "gb18030"):
        try:
            return [
                normalized
                for raw in SOURCE_FILE.read_text(encoding=encoding).splitlines()
                if (normalized := normalize_line(raw))
            ]
        except UnicodeDecodeError:
            continue
    return []


def mysql_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )


def ensure_control_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
              control_key VARCHAR(64) NOT NULL,
              control_value VARCHAR(255) NOT NULL,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (control_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            f"""
            INSERT IGNORE INTO {CONTROL_TABLE}(control_key, control_value)
            VALUES (%s, %s)
            """,
            (CONTROL_KEY, "1" if START_ENABLED else "0"),
        )


def set_initial_generation_state() -> None:
    if not CONTROL_ENABLED or not RESET_CONTROL_ON_START:
        return

    try:
        with mysql_conn() as conn:
            ensure_control_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {CONTROL_TABLE}(control_key, control_value)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE control_value = VALUES(control_value)
                    """,
                    (CONTROL_KEY, "1" if START_ENABLED else "0"),
                )
        print(f"Producer control reset on start: {CONTROL_KEY}={1 if START_ENABLED else 0}")
    except Exception as exc:
        print(f"Producer control reset failed: {exc}")


def generation_enabled() -> bool:
    global _last_control_poll, _cached_generation_enabled

    if not CONTROL_ENABLED:
        return True

    now = time.time()
    if now - _last_control_poll < CONTROL_POLL_SECONDS:
        return _cached_generation_enabled

    _last_control_poll = now
    try:
        with mysql_conn() as conn:
            ensure_control_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT control_value FROM {CONTROL_TABLE} WHERE control_key=%s",
                    (CONTROL_KEY,),
                )
                row = cur.fetchone()
        value = str(row[0]).strip().lower() if row else "0"
        _cached_generation_enabled = value in {"1", "true", "yes", "on", "start", "running"}
    except Exception as exc:
        _cached_generation_enabled = False
        print(f"Producer control is not ready: {exc}")

    return _cached_generation_enabled


def connect() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: value.encode("utf-8"),
                acks="all",
            )
        except Exception as exc:
            print(f"Kafka is not ready: {exc}")
            time.sleep(3)


def main() -> None:
    producer = connect()
    lines = source_lines()
    index = 0
    mode = "file replay" if lines else "dynamic synthetic"
    print(
        f"Producing to {TOPIC}; mode={mode}; source rows={len(lines)}; "
        f"dynamic_titles={DYNAMIC_TITLES}; unique_every={UNIQUE_EVERY}; pool={TOPIC_POOL_SIZE}; "
        f"run_label={RUN_LABEL if INCLUDE_RUN_LABEL else 'off'}; "
        f"control_enabled={CONTROL_ENABLED}; start_enabled={START_ENABLED}; "
        f"reset_control_on_start={RESET_CONTROL_ON_START}"
    )
    set_initial_generation_state()
    last_wait_log = 0.0
    while True:
        if not generation_enabled():
            now = time.time()
            if now - last_wait_log >= 10:
                print("Producer is paused; click the dashboard start button to generate news.")
                last_wait_log = now
            time.sleep(INTERVAL)
            continue

        message = lines[index % len(lines)] if lines else generated_line(index)
        producer.send(TOPIC, message).get(timeout=10)
        print(message)
        index += 1
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
