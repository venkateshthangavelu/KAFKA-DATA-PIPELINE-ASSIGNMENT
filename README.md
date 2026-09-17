# Kafka Data Pipeline Assignment

This repository contains a small end-to-end validation framework for a customer account Kafka pipeline.

## Overview

The application flow is:

Customer events -> Kafka topic -> Application consumer -> Transformation -> PostgreSQL -> Metrics API

The framework does not build or modify the application. It validates the behavior from the tester's perspective.

## Goals

- Load sample customer events from JSON
- Publish them to Kafka
- Consume them back from a test consumer group
- Validate schema and payload quality
- Wait for processing
- Reconcile Kafka messages with PostgreSQL records
- Validate business rules such as minor and employee flags
- Check the metrics REST API
- Produce an HTML test report

## Project structure

```text
.
├── docker-compose.yml
├── .env.example
├── PROJECT_NOTES.md
├── README.md
├── data-test/
│   ├── README.md
│   ├── pytest.ini
│   ├── requirements.txt
│   ├── run_tests.sh
│   ├── config/
│   │   └── test_config.yaml
│   ├── reports/
│   ├── testdata/
│   │   ├── customer-events.json
│   │   └── customer-event-schema.json
│   ├── tests/
│   │   └── test_customer_account.py
│   └── utils/
│       ├── __init__.py
│       ├── db_utils.py
│       └── kafka_utils.py
├── .gitignore
└── .venv/
```

## Environment setup

### 1. Start Kafka and PostgreSQL

```bash
docker compose up -d
```

### 2. Create Python environment

```bash
cd data-test
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the tests

```bash
pytest -q --html=reports/customer_account_report.html
```

Or use:

```bash
bash run_tests.sh
```

## Validations included

- Kafka message count equals expected count
- JSON schema validates successfully
- PostgreSQL record count matches expected output
- Source-to-target matching by customerId and accountId
- `minor_flag` calculated independently
- `employee_flag` calculated independently
- Metrics API responds successfully
- Duplicate/null checks for core fields

## Client-facing summary

This project is a compact, assignment-style data pipeline validation framework. It proves the end-to-end flow of customer data from Kafka into PostgreSQL and confirms that the application behavior matches the expected business rules with minimal complexity and easy maintainability.

READ records from PostgreSQL

ASSERT DB count = 5

FOR each input message:
    FIND matching DB record using customerId

    ASSERT customerId matches
    ASSERT accountId matches
    ASSERT name matches

    CALCULATE expected minor_flag
    ASSERT DB minor_flag = expected value

    CALCULATE expected employee_flag
    ASSERT DB employee_flag = expected value

CHECK raw archive

CALL metrics API

ASSERT API status = 200
ASSERT topic is correct
ASSERT message count is correct

GENERATE test report
```

---

# 7. `utils/kafka_utils.py`

This contains only basic Kafka operations.

It should provide:

- producer creation
- message production
- consumer creation
- message consumption

### Pseudocode

```text
FUNCTION create_producer:
    CONNECT to Kafka
    RETURN producer

FUNCTION produce_messages(topic, messages):
    CREATE producer

    FOR each message:
        convert message to JSON
        publish message to topic

    flush producer

    RETURN number of messages

FUNCTION consume_messages(topic, groupId, expectedCount):
    CREATE consumer
    USE separate test consumer group
    SUBSCRIBE to topic

    START timeout

    WHILE received messages < expectedCount:
        POLL Kafka

        IF message received:
            convert JSON to object
            add to list

        IF timeout reached:
            STOP

    CLOSE consumer

    RETURN messages
```

No Kafka factory or Kafka abstraction layer is required.

---

# 8. `utils/db_utils.py`

This contains basic PostgreSQL access.

### Pseudocode

```text
FUNCTION get_connection:
    CONNECT to PostgreSQL
    RETURN connection

FUNCTION get_customer_records:
    OPEN DB connection

    EXECUTE:
        SELECT customer_id,
               account_id,
               name,
               minor_flag,
               employee_flag
        FROM public.customer_account_profile

    RETURN rows

FUNCTION get_record_count:
    OPEN DB connection

    EXECUTE:
        SELECT COUNT(*)
        FROM public.customer_account_profile

    RETURN count
```

For this assignment, direct SQL/JDBC-style access is preferable to an ORM.

---

# 9. `testdata/customer-events.json`

Contains the five input messages.

Example:

```text
C001 / A001 / John    / 2010-05-10 / null
C002 / A002 / David   / 1990-08-20 / E100
C003 / A003 / Sarah   / 2008-03-15 / null
C004 / A004 / Michael / 1985-11-12 / E200
C005 / A005 / Robert  / 1995-01-25 / null
```

The important point is that the data contains both:

- minor/adult customers
- employee/non-employee customers

### Pseudocode

```text
JSON ARRAY

message 1
message 2
message 3
message 4
message 5
```

No Java/Python object creation is necessary just to hold these five records.

---

# 10. `testdata/customer-event-schema.json`

Contains the JSON schema supplied by the assignment.

### Pseudocode

```text
DEFINE required fields:
    customerId
    accountId
    name
    dateOfBirth

DEFINE optional field:
    employeeId

DEFINE date format:
    YYYY-MM-DD

DO NOT allow additional fields
```

The main test loads this schema and validates every consumed message.

---

# 11. `config/test_config.yaml`

Contains environment-specific values.

Example:

```text
kafka:
    bootstrap server
    topic
    test consumer group

database:
    host
    port
    database
    username
    password

api:
    base URL

test:
    expected message count = 5
```

### Pseudocode

```text
READ YAML

GET Kafka configuration
GET database configuration
GET API configuration
GET expected message count
```

No configuration framework is required.

---

# 12. `requirements.txt`

Contains only the required Python dependencies.

```text
pytest
pytest-html
confluent-kafka
psycopg2-binary
requests
jsonschema
PyYAML
```

### Pseudocode

```text
INSTALL dependencies from requirements.txt
```

---

# 13. `README.md`

Documents:

- prerequisites
- how to install dependencies
- Kafka/database/API configuration
- how to run tests
- assumptions
- expected result
- limitations

### Pseudocode

```text
Explain setup
Explain configuration
Explain test command
Explain assumptions
Explain report location
```

---

# 14. Python Test Execution

Example:

```text
pytest -v --html=reports/test-report.html --self-contained-html
```

The result is an HTML report.

No custom reporting utility is necessary.

---

# 15. Important Python Testing Detail – Asynchronous Processing

Kafka processing is asynchronous.

Therefore this is not ideal:

```text
produce messages
sleep 5 seconds
query database
```

A better simple approach is:

```text
produce messages

WHILE DB count < 5:
    wait briefly
    check DB count

IF count becomes 5:
    continue

IF timeout reached:
    fail test
```

This is enough.

We do not need a generic retry framework.

---

# 16. Important Kafka Test Detail

The application uses:

```text
customer-account-consumer-group
```

The test should use:

```text
customer-account-test-group
```

The reason is to prevent the application and test consumer from competing for messages.

For repeated runs, a unique test consumer group can be used:

```text
customer-account-test-<timestamp>
```

This prevents old offsets from interfering with a new test.

---

# 17. Important Database Test Detail

Repeated test execution can create duplicate database rows.

For this small assignment, use one of these simple approaches:

```text
Option 1:
Clean test table before test

OR

Option 2:
Use unique test customer IDs and validate only those records

OR

Option 3:
Use a dedicated test database
```

The choice depends on the available test environment.

Do not perform uncontrolled cleanup on production-like data.