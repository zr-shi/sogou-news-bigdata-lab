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

DEFAULT_TOPICS = [
    "人工智能",
    "新能源汽车",
    "大学生就业",
    "低空经济",
    "体育赛事",
    "电影票房",
    "网络安全",
    "大数据分析",
    "智慧医疗",
    "旅游消费",
]


def normalize_line(line: str):
    fields = [field.strip() for field in line.strip().split(",")]
    if len(fields) != 6:
        return None
    return ",".join(fields)


def generated_line() -> str:
    now = datetime.now()
    return ",".join(
        [
            now.strftime("%H:%M:%S"),
            str(random.randint(100000, 999999)),
            random.choice(DEFAULT_TOPICS),
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
    print(f"Producing to {TOPIC}; source rows={len(lines)}")
    while True:
        message = lines[index % len(lines)] if lines else generated_line()
        producer.send(TOPIC, message).get(timeout=10)
        print(message)
        index += 1
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
