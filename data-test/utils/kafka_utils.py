import json
import time
from typing import Any, List

from confluent_kafka import Consumer, Producer


def build_producer(bootstrap_servers: str):
    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "customer-account-producer"
    }
    return Producer(**config)


def produce_messages(messages: List[dict], bootstrap_servers: str, topic: str):
    producer = build_producer(bootstrap_servers)
    delivered = 0
    for message in messages:
        producer.produce(topic, value=json.dumps(message).encode("utf-8"))
        producer.flush(timeout=10)
        delivered += 1
    return delivered


def build_consumer(bootstrap_servers: str, group_id: str, auto_offset_reset: str = "earliest"):
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
    }
    consumer = Consumer(config)
    return consumer


def consume_messages(consumer: Consumer, topic: str, timeout_seconds: int = 30, expected_count: int = 5):
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout_seconds

    while time.time() < deadline and len(messages) < expected_count:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(f"Kafka error: {msg.error()}")
        payload = json.loads(msg.value().decode("utf-8"))
        messages.append(payload)

    consumer.close()
    return messages
