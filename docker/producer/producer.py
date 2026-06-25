import os
import random
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "sougoulogs")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5"))
SOURCE_FILE = Path(os.getenv("SOURCE_FILE", "/data/sougou.log"))

DYNAMIC_TITLES = os.getenv("PRODUCER_DYNAMIC_TITLES", "true").strip().lower() not in {"0", "false", "no", "off"}
UNIQUE_EVERY = max(1, int(os.getenv("PRODUCER_UNIQUE_EVERY", "5")))
TOPIC_POOL_SIZE = max(10, int(os.getenv("PRODUCER_TOPIC_POOL_SIZE", "200")))
INCLUDE_RUN_LABEL = os.getenv("PRODUCER_INCLUDE_RUN_LABEL", "true").strip().lower() not in {"0", "false", "no", "off"}
RUN_LABEL = os.getenv("PRODUCER_RUN_LABEL") or datetime.now().strftime("%Y%m%d%H%M%S")

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
    topic_index = bucket % TOPIC_POOL_SIZE
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
        f"run_label={RUN_LABEL if INCLUDE_RUN_LABEL else 'off'}"
    )
    while True:
        message = lines[index % len(lines)] if lines else generated_line(index)
        producer.send(TOPIC, message).get(timeout=10)
        print(message)
        index += 1
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
