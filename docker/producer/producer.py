import os
import random
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

TITLE_SUFFIXES = [
    "最新进展",
    "行业观察",
    "政策解读",
    "用户热议",
    "市场表现",
    "技术突破",
    "年度报告",
    "应用案例",
    "趋势分析",
    "深度调查",
]


def normalize_line(line: str):
    fields = [field.strip() for field in line.strip().split(",")]
    if len(fields) != 6:
        return None
    return ",".join(fields)


def generated_title(index: int) -> str:
    if not DYNAMIC_TITLES:
        return random.choice(BASE_TOPICS[:10])

    bucket = index // UNIQUE_EVERY
    topic_index = bucket if TOPIC_POOL_SIZE <= 0 else bucket % TOPIC_POOL_SIZE
    base = BASE_TOPICS[topic_index % len(BASE_TOPICS)]
    suffix = TITLE_SUFFIXES[(topic_index // len(BASE_TOPICS)) % len(TITLE_SUFFIXES)]
    serial = f"{topic_index + 1:04d}"
    if INCLUDE_RUN_LABEL:
        serial = f"{RUN_LABEL}-{serial}"
    return f"{base}-{suffix}-{serial}"


def generated_line(index: int) -> str:
    now = datetime.now()
    return ",".join(
        [
            now.strftime("%H:%M:%S"),
            str(random.randint(100000, 999999)),
            generated_title(index),
            str(random.randint(1, 50)),
            str(random.randint(1, 20)),
            random.choice(["web", "app", "search"]),
        ]
    )


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
