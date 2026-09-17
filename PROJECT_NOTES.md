# Kafka Data Pipeline Assignment

## Project overview
This project is a lightweight end-to-end validation framework for a customer account Kafka pipeline. It confirms that customer events are produced to Kafka, processed by the application, stored in PostgreSQL, and exposed through the metrics API.

## Objective
Provide a small, easy-to-understand test framework that verifies the integrity of data as it moves from source to target without rewriting the application itself.

## Main workflow
1. Load sample customer events from JSON.
2. Publish the messages to Kafka.
3. Consume the same messages using a separate test consumer group.
4. Validate the JSON schema.
5. Wait until the app processes and writes records to the database.
6. Reconcile source and target data.
7. Check business flags such as minor_flag and employee_flag.
8. Validate the REST metrics endpoint.
9. Produce a test report.

## Architecture
- Source data: JSON customer events
- Messaging layer: Kafka topic `customer.account.events.v1`
- Processing layer: application consumer/transformation
- Data store: PostgreSQL table `public.customer_account_profile`
- API layer: metrics endpoint for Kafka or pipeline status

## Key business rules validated
- Minor flag: `Y` if age < 18, otherwise `N`
- Employee flag: `Y` if employeeId is present, otherwise `N`

## Why this framework is useful
- Fast to set up
- Easy for testers to understand
- Focused on source-to-target reconciliation
- Suitable for small assignment-style data pipeline validation
