# KAFKA-DATA-PIPELINE-ASSIGNMENT

# Kafka Data Pipeline Assignment

This project contains a small end-to-end test setup for a customer account Kafka pipeline.

The test flow is simple:

1. Load sample customer events from JSON.
2. Publish the events to Kafka.
3. Consume the events back from a separate test consumer group.
4. Validate the message structure.
5. Wait for the application to process the events.
6. Check the resulting database records and API behavior.

## Project Layout

- `data-test/tests/` contains the main pipeline test.
- `data-test/utils/` contains Kafka and database helper functions.
- `data-test/testdata/` contains sample JSON input.
- `data-test/config/test_config.yaml` contains Kafka, database, and API settings.

## Requirements

Install the Python packages listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Running the Test

From the repository root, run:

```bash
pytest data-test/tests/test_customer_account.py
```

## Configuration

Update `data-test/config/test_config.yaml` if your local Kafka broker, database, or API uses different values.

## Notes

- The test uses its own Kafka consumer group so it does not compete with the application consumer.
- The expected event count is defined in `data-test/config/test_config.yaml`.
