# Case Audit Data Pipeline

## Overview
This project implements an automated ETL pipeline to audit case management and family counseling records. It extracts client and event data from a PostgreSQL database, applies data validation checks, generates detailed Excel audit reports, and automates report delivery via email.

Originally developed for a social service organization of approx 70 case managers and counselors to ensure data quality and compliance in client records, the pipeline is modular, fault-tolerant, and designed for scheduled execution.

## Features
- **Data Extraction**: SQL queries to extract documents and events related to case management and counseling services.
- **Data Transformation**: Data cleaning, type enforcement, and quality assurance (e.g., checking for missing signatures, document completion percentages).
- **Report Generation**: Excel reports with conditional formatting to highlight missing or incomplete information.
- **Automated Notification**: Email delivery of audit reports to individual case managers and supervisors via the Mailgun API.
- **Retry Logic**: Built-in retry mechanism for database connections to handle transient failures.
- **Configuration**: Centralized configuration for database connections and file paths.

## Tech Stack
- **Python**: Data processing and automation
- **PostgreSQL**: Data source
- **Pandas**: Data wrangling
- **OpenPyXL / XlsxWriter**: Excel report generation
- **Mailgun API**: Email notifications
- **SQL**: Data extraction logic

## Project Structure
```
case-audit-data-pipeline/
├── config.py                      # Configuration and settings
├── database_connector.py          # PostgreSQL connection and query executor
├── report_generator.py            # Excel report generator
├── email_notifier.py               # Email sending utility
├── pipeline_runner.py              # Main script to orchestrate the ETL process
├── queries/
│   ├── case_mgmt_documents_query.sql
│   ├── case_mgmt_events_query.sql
│   ├── family_counseling_documents_query.sql
│   └── family_counseling_events_query.sql
├── requirements.txt                # Project dependencies
├── README.md
```

## Setup
1. Clone the repository.
2. Create a `.env` file based on `.env.example` and insert your Mailgun credentials.
3. Install the dependencies:
    ```
    pip install -r requirements.txt
    ```
4. Configure your database connection in `config.py`.
5. Execute the pipeline:
    ```
    python pipeline_runner.py
    ```

## Note
Due to client confidentiality, database credentials, raw data, and email API keys are excluded.

## License
[MIT License](LICENSE)
