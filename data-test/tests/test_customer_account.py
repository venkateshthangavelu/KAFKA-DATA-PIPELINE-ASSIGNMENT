import json
import os
import time
from datetime import date
from pathlib import Path

import pytest
import requests
import yaml
from jsonschema import Draft202012Validator

from utils.db_utils import count_records, fetch_customer_records
from utils.kafka_utils import build_consumer, consume_messages, produce_messages


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "test_config.yaml"
DATA_PATH = ROOT / "testdata" / "customer-events.json"
SCHEMA_PATH = ROOT / "testdata" / "customer-event-schema.json"
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def calculate_minor_flag(dob: str):
    dob_date = date.fromisoformat(dob)
    today = date.today()
    age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
    return "Y" if age < 18 else "N"


def calculate_employee_flag(employee_id):
    return "Y" if employee_id is not None else "N"


def wait_for_records(expected_count: int, timeout_seconds: int, db_config: dict):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count = count_records(db_config)
        if count >= expected_count:
            return True
        time.sleep(2)
    return False


@pytest.mark.parametrize("sample", ["customer-account-flow"])
def test_customer_account_pipeline(sample):
    config = load_yaml(str(CONFIG_PATH))
    messages = load_json(str(DATA_PATH))
    schema = load_json(str(SCHEMA_PATH))

    kafka_cfg = config["kafka"]
    db_cfg = config["database"]
    api_cfg = config["api"]
    validation_cfg = config["validation"]

    produced_count = produce_messages(messages, kafka_cfg["bootstrap_servers"], kafka_cfg["topic"])
    assert produced_count == len(messages), "Kafka producer did not send all messages"

    consumer = build_consumer(
        kafka_cfg["bootstrap_servers"],
        kafka_cfg["group_id"],
        auto_offset_reset="earliest",
    )
    consumed = consume_messages(consumer, kafka_cfg["topic"], timeout_seconds=kafka_cfg["timeout_seconds"], expected_count=len(messages))
    assert len(consumed) == len(messages), "Not all Kafka messages were consumed to validate the topic"

    validator = Draft202012Validator(schema)
    for message in consumed:
        validator.validate(message)

    assert wait_for_records(validation_cfg["expected_record_count"], validation_cfg["wait_timeout_seconds"], db_cfg), (
        "Database did not receive expected records within the timeout"
    )

    db_records = fetch_customer_records(db_cfg)
    assert len(db_records) == validation_cfg["expected_record_count"], "Database record count mismatch"

    records_by_customer = {record["customer_id"]: record for record in db_records}
    for message in messages:
        customer_id = message["customerId"]
        assert customer_id in records_by_customer, f"Customer {customer_id} is missing in database"

        db_record = records_by_customer[customer_id]
        assert db_record.get("account_id") == message["accountId"], f"accountId mismatch for {customer_id}"
        assert db_record.get("name") == message["name"], f"name mismatch for {customer_id}"
        assert db_record.get("minor_flag") == calculate_minor_flag(message["dob"]), f"minor_flag mismatch for {customer_id}"
        assert db_record.get("employee_flag") == calculate_employee_flag(message["employeeId"]), f"employee_flag mismatch for {customer_id}"

    raw_archive_response = requests.get(f"{api_cfg['base_url']}{api_cfg['metrics_endpoint']}", timeout=10)
    assert raw_archive_response.status_code == 200, "Metrics API returned non-200 status"
    metrics = raw_archive_response.json()
    assert metrics.get("topic") == kafka_cfg["topic"], "Metrics topic mismatch"
    assert metrics.get("message_count") == validation_cfg["expected_record_count"], "Metrics count mismatch"

    # Optional quality checks for nulls/blanks and duplicates
    ids = [record["customer_id"] for record in db_records]
    assert len(ids) == len(set(ids)), "Duplicate customer_id records detected"
    assert all(record.get("customer_id") for record in db_records), "Null/blank customer_id detected"
    assert all(record.get("account_id") for record in db_records), "Null/blank account_id detected"

    print(f"Validated {len(db_records)} database records and API metrics successfully.")
