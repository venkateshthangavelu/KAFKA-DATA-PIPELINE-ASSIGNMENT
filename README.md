# Kafka Data Pipeline Assignment

This repository contains a small end-to-end validation framework for a customer account Kafka pipeline.

## Overview

The application flow is:

Customer events -> Kafka topic -> Application consumer -> Transformation -> PostgreSQL -> Metrics API

The framework does not build or modify the application. Instead, it validates the end-to-end behavior from the tester's perspective.

## Objective

The goal is to validate that customer data is correctly:

- generated from sample input events,
- published to Kafka,
- consumed and processed by the application,
- written to PostgreSQL,
- reconciled against expected business rules,
- exposed by the metrics API,
- reported through a simple HTML test report.

## Scope

This project intentionally stays small and easy to understand. It is not designed as a generic enterprise framework.

Current scope:

- 1 Kafka topic: customer.account.events.v1
- 1 Kafka partition
- 5 test messages
- 1 PostgreSQL table: public.customer_account_profile
- 1 main transformation
- 1 raw archive file
- 1 metrics REST API

## Validation flow

The test flow is:

1. Read five test messages from JSON.
2. Publish them to Kafka.
3. Consume the messages using a separate test consumer group.
4. Validate the JSON schema and message structure.
5. Wait until the application processes the messages.
6. Query PostgreSQL.
7. Validate record count.
8. Match Kafka records to database records.
9. Validate field mappings.
10. Independently calculate minor_flag.
11. Independently calculate employee_flag.
12. Validate the raw message archive.
13. Call the metrics API.
14. Validate the API response.
15. Generate an HTML test report.

The primary validation is source-to-target reconciliation.

Example:

```text
Kafka customerId  = C001
        |
        v
DB customer_id    = C001

Kafka accountId   = A001
        |
        v
DB account_id     = A001

Kafka name        = John
        |
        v
DB name           = John

DOB
        |
        v
minor_flag        = independently calculated expected value

employeeId
        |
        v
employee_flag     = independently calculated expected value
```

## Business rules used by the test

The assignment defines minor_flag and employee_flag. If the exact business rules are not documented, these should be treated as assumptions.

### Minor flag

```text
age < 18  -> Y
age >= 18 -> N
```

The test calculates age itself from the date of birth.

### Employee flag

```text
employeeId != null -> Y
employeeId == null  -> N
```

The test calculates this value independently instead of calling the application transformation logic.

## Test automation approach

Python is a good fit for this small project because it allows the Kafka, database, and API checks to be implemented with minimal code.

Main libraries:

- pytest — test execution and assertions
- pytest-html — HTML report generation
- confluent-kafka — Kafka producer and consumer
- psycopg2-binary — PostgreSQL connectivity
- requests — REST API calls
- jsonschema — JSON schema validation
- PyYAML — configuration loading

This project does not require pandas or a large enterprise framework for five records.

## Repository structure

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

## Python file responsibilities

### tests/test_customer_account.py

This is the main test file. It should contain the actual scenario logic, including:

- loading test data,
- producing Kafka messages,
- consuming Kafka messages,
- validating schema,
- waiting for database processing,
- querying PostgreSQL,
- comparing source and target data,
- computing expected flags,
- validating API response,
- generating the final report.

### utils/kafka_utils.py

This contains only the basic Kafka operations:

- producer creation,
- message production,
- consumer creation,
- message consumption.

### utils/db_utils.py

This contains the basic PostgreSQL access:

- connection creation,
- customer record retrieval,
- record count checks.

### testdata/customer-events.json

This file contains the five sample messages used by the test.

### testdata/customer-event-schema.json

This file defines the expected message schema.

### config/test_config.yaml

This file stores environment-specific values such as Kafka, database, and API settings.

### requirements.txt

This file includes the minimum Python dependencies required for the test framework.

## Environment setup

### 1. Start Kafka and PostgreSQL

```bash
docker compose up -d
```

### 2. Create a Python environment

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

Or use the helper script:

```bash
bash run_tests.sh
```

## Configuration example

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "customer.account.events.v1"
  group_id: "customer-account-test-group"

database:
  host: "localhost"
  port: 5432
  database: "customerdb"
  user: "postgres"
  password: "postgres"

api:
  base_url: "http://localhost:8080"
  metrics_endpoint: "/api/metrics"

validation:
  expected_record_count: 5
```

## Important testing notes

### Asynchronous processing

Kafka processing is asynchronous. A fixed sleep is not the best approach. Instead, the test should poll the database until the expected count is reached or a timeout occurs.

Example logic:

```text
PRODUCE messages

WHILE DB count < expected count:
    wait briefly
    check DB count

IF timeout reached:
    fail test
```

### Kafka consumer groups

The application uses its own consumer group. The test should use a separate group to avoid competing for the same messages.

Recommended pattern:

```text
application group: customer-account-consumer-group
test group: customer-account-test-group
```

For repeated runs, use a unique group name such as:

```text
customer-account-test-<timestamp>
```

### Database cleanup for repeated runs

Repeated test execution may create duplicate rows. For a small assignment, one of these approaches is acceptable:

- clean the target test table before the test,
- use unique test customer IDs, or
- use a dedicated test database.

Do not perform uncontrolled cleanup on production-like data.

## Client-facing summary

This project is a compact, assignment-style data pipeline validation framework. It proves the end-to-end flow of customer data from Kafka into PostgreSQL and verifies that the application's behavior matches the expected business rules with minimal complexity and easy maintainability.

## Expected result

A successful test run should confirm that:

- all messages were produced,
- all messages were consumed,
- the JSON schema passed,
- PostgreSQL contains the expected rows,
- field values match source data,
- minor_flag and employee_flag are correct,
- the metrics API responds successfully,
- the HTML report is generated.
