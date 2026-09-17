# Kafka Data Pipeline Test Framework

This project is a lightweight Python test automation framework for validating a Kafka-to-PostgreSQL customer account pipeline.

## Purpose

The framework tests the flow:

1. Load example customer events from JSON
2. Send them to Kafka
3. Consume them back using a test consumer group
4. Validate the event schema and payload shape
5. Wait for the application to process the records
6. Query PostgreSQL and verify reconciliation
7. Check business-rule flags such as `minor_flag` and `employee_flag`
8. Validate the API metrics endpoint
9. Generate a report for the tester

## Folder structure

```text
data-test/
├── config/
│   └── test_config.yaml
├── reports/
├── testdata/
│   ├── customer-events.json
│   └── customer-event-schema.json
├── tests/
│   └── test_customer_account.py
├── utils/
│   ├── db_utils.py
│   └── kafka_utils.py
├── .gitignore
├── README.md
├── requirements.txt
└── run_tests.sh
```

## Quick start

```bash
cd data-test
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q --html=reports/customer_account_report.html
```

## Assumptions

- Kafka is running at `localhost:9092`
- PostgreSQL is running at `localhost:5432`
- Application metrics API is available at `http://localhost:8080/api/metrics`
- The pipeline application is already deployed and running externally

## Notes

This is intentionally a small framework designed for a workshop-style assignment, not a large enterprise test framework.
