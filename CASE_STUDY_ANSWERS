CASE STUDY SOLUTION ﻿along with End-to-end architecture for Mortgage Application & Underwriting Pipeline case study, using Snowflake, DBT, Apache Airflow, and Terraform.

I  would design the mortgage underwriting pipeline using a Snowflake-based Medallion Architecture. Snowpipe handles ingestion, dbt manages transformations, and Airflow orchestrates dependencies. The Bronze layer stores raw source data, Silver performs cleansing, deduplication, validation, and PII protection, and Gold calculates LTV and DTI and generates underwriting decisions. I also considered Snowflake RBAC, dynamic masking, dbt data quality tests, and Terraform for infrastructure provisioning.

[Source Systems]
     |
     v
[Bronze Layer]
(Raw ingested data - applications, credit bureau, transactions)
     |
     v
[Silver Layer]
(Cleansed, joined, and enriched datasets with calculated fields)
     |
     v
[Gold Layer]
(Business-level aggregates - LTV, DTI, approved applicants report)
     |
     v
[BI/Analytics Tools, ML Models, Reporting]

To Ingest Snowflake Data-

I would land the JSON files in cloud object storage, such as Amazon S3, and use a Snowflake stage with COPY INTO for batch ingestion or Snowpipe for continuous ingestion.

The steps are:
1. Receive the JSON files and validate file arrival.
2. Create a Snowflake stage and file format.
3. Load the records into a Bronze table, preserving the raw payload in a VARIANT column.
4. Extract fields and flatten nested arrays when needed.
5. Apply schema validation, null checks, deduplication, and data quality rules in dbt.
6. Load validated records into Silver tables.
CREATE OR REPLACE FILE FORMAT mortgage_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE TABLE bronze_mortgage_applications (
    raw_data VARIANT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

I would create a CSV file format, configure the stage, and load the data into a raw Bronze table.
CREATE OR REPLACE FILE FORMAT credit_csv_format
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE TABLE bronze_credit_bureau (
    applicant_id VARCHAR,
    ssn VARCHAR,
    fico_score NUMBER,
    monthly_debt NUMBER(12,2),
    ingestion_timestamp TIMESTAMP_NTZ
);
To handle Nested JSON and Parsing-

SELECT
    raw_data:applicant_id::VARCHAR AS applicant_id,
    raw_data:loan_amount::NUMBER(12,2) AS loan_amount,
    raw_data:property_value::NUMBER(12,2) AS property_value
FROM bronze_mortgage_applications;

Biquery Data Ingestion

SELECT
  JSON_VALUE(payload, '$.applicant_id') AS applicant_id,
  SAFE_CAST(
    JSON_VALUE(payload, '$.loan_amount') AS NUMERIC
  ) AS loan_amount
FROM `project.dataset.mortgage_applications`;

SQL and Underwriting Business Rules-
FOR CALCULATING LTV-
SELECT
    applicant_id,
    loan_amount,
    property_value,
    ROUND(loan_amount / NULLIF(property_value, 0) * 100, 2) AS ltv,
    CASE
        WHEN property_value IS NULL OR property_value <= 0
            THEN 'TECHNICAL_REJECTION'
        WHEN loan_amount / property_value > 0.95
            THEN 'DECLINED_LTV_EXCEEDED'
        ELSE 'PASS'
    END AS ltv_status
FROM silver_mortgage_applications;

FOR CALCULATING DTI-

SELECT
    applicant_id,
    monthly_debt,
    monthly_gross_income,
    ROUND(
        monthly_debt / NULLIF(monthly_gross_income, 0) * 100, 2
    ) AS dti
FROM silver_mortgage_applications;

TO COMPUTE BOTH LTV & DTI-

WITH metrics AS (
    SELECT
        applicant_id,
        loan_amount,
        property_value,
        monthly_debt,
        monthly_gross_income,
        fico_score,
        loan_amount / NULLIF(property_value, 0) * 100 AS ltv,
        monthly_debt / NULLIF(monthly_gross_income, 0) * 100 AS dti
    FROM silver_mortgage_applications
),
decisions AS (
    SELECT *,
        CASE
            WHEN property_value IS NULL OR property_value <= 0
                THEN 'TECHNICAL_REJECTION'
            WHEN monthly_gross_income IS NULL
                 OR monthly_gross_income <= 0
                THEN 'TECHNICAL_REJECTION'
            WHEN fico_score IS NULL
                THEN 'TECHNICAL_REJECTION'
            WHEN ltv > 95
                THEN 'DECLINED_LTV_EXCEEDED'
            WHEN dti > 43
                THEN 'DECLINED_DTI_EXCEEDED'
            WHEN fico_score < 620
                THEN 'DECLINED_LOW_CREDIT_SCORE'
            WHEN fico_score BETWEEN 620 AND 660
                 AND dti BETWEEN 36 AND 43
                THEN 'REQUIRES_CO_SIGNER'
            ELSE 'APPROVED'
        END AS decision_reason
    FROM metrics
)
SELECT *
FROM decisions;

HANDLE DUPLICATE RECORDS-
SELECT *
FROM bronze_credit_bureau
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY applicant_id
    ORDER BY ingestion_timestamp DESC
) = 1;

JOIN MORTGAGE APPLICATION WITH CREDIT BUREAU DATA-
SELECT
    a.applicant_id,
    a.loan_amount,
    a.property_value,
    a.monthly_gross_income,
    c.fico_score,
    c.monthly_debt
FROM bronze_mortgage_applications a
LEFT JOIN bronze_credit_bureau c
    ON a.applicant_id = c.applicant_id;

    DBT PROJECT STRUCTURE-

    models/
├── staging/
│   ├── stg_mortgage_applications.sql
│   ├── stg_credit_bureau.sql
│   └── sources.yml
├── intermediate/
│   └── int_applicant_financial_profile.sql
└── marts/
    ├── fct_underwriting_decisions.sql
    ├── fct_technical_rejections.sql
    └── hmda_reporting.sql

    DBT TESTS TO BE IMPLEMENTED-

    models:
  - name: fct_underwriting_decisions
    columns:
      - name: applicant_id
        data_tests:
          - not_null
      - name: decision_reason
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values:
                  - APPROVED
                  - REQUIRES_CO_SIGNER
                  - DECLINED_LTV_EXCEEDED
                  - DECLINED_DTI_EXCEEDED
                  - DECLINED_LOW_CREDIT_SCORE
                  - TECHNICAL_REJECTION

            MASKING RULES LOGIC-
1.CREATE OR REPLACE MASKING POLICY ssn_mask
AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN IS_ROLE_IN_SESSION('COMPLIANCE_OFFICER')
            THEN val
        ELSE 'XXX-XX-' || RIGHT(val, 4)
    END;
       
APPLY POLICY
ALTER TABLE silver_mortgage_applications
MODIFY COLUMN ssn
SET MASKING POLICY ssn_mask;

AIRFLOW ORCHESTRATION-
Start DAG
Task A

Load mortgage JSON


Task B

Load credit CSV



Both succeed → Run dbt Silver
Run dbt Gold and quality tests
Publish results and monitor status

TIMETRAVEL-
SELECT *
FROM silver_mortgage_applications
AT (
    TIMESTAMP =>
        '2026-09-29 10:30:00'::TIMESTAMP_NTZ
)
WHERE applicant_id = 'APP123';

To handle a 1,000x spike in mortgage applications over a weekend?
Answer:

I would design the pipeline for elastic ingestion, parallel processing, and controlled concurrency.
1. Use cloud object storage and Snowpipe or other suitable ingestion mechanisms to absorb incoming files.
2. Scale Snowflake virtual warehouses according to workload, using auto-scaling or multi-cluster warehouses where appropriate.
3. Use incremental dbt models to avoid repeatedly processing all historical records.
4. Partition workloads logically by ingestion date or another suitable processing key, without creating unnecessary small tasks.
5. Monitor queueing, warehouse utilization, query latency, ingestion lag, and failure rates.
6. Configure retry policies and idempotent processing so that retries do not duplicate decisions.
7. Load-test the system against expected peak volumes and establish capacity and cost alerts.
8. 
DETAILED DESCRIPTION-
1.Components for Mortgage Application & Underwriting Pipeline case study, using Snowflake, DBT, Apache Airflow, and Terraform are-
  Data sources: Mortgage application JSON files and credit bureau CSV files.
  Ingestion: Load data into Snowflake using stages, COPY INTO, or Snowpipe.
  Bronze: Preserve raw data and ingestion timestamps.
  Silver: Parse JSON, join credit records, validate data, deduplicate, and protect PII.
  Gold: Calculate LTV and DTI, apply underwriting rules, and generate decision reason codes and reporting tables.
  Orchestration: Airflow manages dependencies, retries, failures, and execution schedules. DBT performs transformations and tests.
  Consumption: BI dashboards, business teams, and authorized compliance auditors access approved outputs.
  Cross-cutting controls: GitHub CI/CD, Terraform infrastructure management, Snowflake RBAC, masking, and monitoring.
Use Snowflake for ELT, DBT for transformations and tests, and Apache Airflow for orchestration. Mortgage JSON and credit bureau CSV land in Bronze. Silver parses and joins the sources, validates critical fields, deduplicates records, and protects PII. Gold applies underwriting rules and exposes reporting-ready decisions and reason codes. Both source loads must succeed before Silver runs.

 2. JSON ingestion into Snowflake-
Land files in cloud object storage, configure a Snowflake stage and JSON file format, then load into a Bronze table with a `VARIANT` payload and ingestion metadata. Use `COPY INTO` for batch loads or Snowpipe for continuous ingestion. Parse scalar values with the `:` path operator and cast to expected types; use `LATERAL FLATTEN` for nested arrays. Route malformed or invalid records to an exception path.

 3. CSV credit-bureau ingestion-
Use a CSV file format, stage the files, and load applicant ID, SSN, FICO score, monthly debt, source filename, and ingestion timestamp into a raw landing table. Validate header/column structure, types, missing identifiers, duplicates, and expected score ranges before downstream processing.

 4. BigQuery JSON parsing-
BigQuery can load newline-delimited JSON from Cloud Storage or query supported JSON data. Use `JSON_VALUE` to extract scalar values, `JSON_QUERY` for JSON objects/arrays, and `SAFE_CAST` for conversion that should not fail the whole query. This case study's target platform is Snowflake, so BigQuery would be an alternative-platform explanation rather than the primary implementation.

 5. LTV calculation
`LTV = (loan_amount / property_value) * 100`. Decline when LTV is greater than 95%. Exactly 95% passes this specific threshold. Property value must be present and greater than zero; otherwise log a technical rejection or invalid-data exception.

 6. DTI calculation
`DTI = (monthly_debt / monthly_gross_income) * 100`. Decline when DTI is greater than 43%. Exactly 43% does not exceed the threshold. Income must be present and positive; do not allow division by zero or silently interpret invalid income as zero.

 7. Underwriting decision precedence
First apply technical validations. Then apply mandatory decline rules for LTV, DTI, and FICO below 620. Apply the optional co-signer rule only when no mandatory decline applies: FICO 620–660 and DTI 36%–43%. Otherwise mark the record approved under the rules specified in this case study. Log a reason code for every outcome.

 8. Technical rejections
Write records with critical missing fields (for example missing FICO or null/invalid property value) to `technical_rejections` with applicant ID, reason code, source, ingestion timestamp, pipeline run ID, and error details. Preserve the raw source record for audit. Reprocess when valid data arrives according to a documented policy.

 9. Joining the sources
Join mortgage applications to bureau data on a stable unique applicant identifier, not SSN. Use a left join from applications so early-arriving applications remain visible while bureau data is delayed. Monitor unmatched applications and ensure duplicate bureau rows are resolved by a defined business rule.

 10. Duplicate handling
Use `ROW_NUMBER()` over the business key and order by a reliable source update timestamp or ingestion timestamp. Keep the latest record only when that is the agreed rule. Preserve raw events; do not treat legitimate historical credit snapshots as duplicates without confirming semantics.

 11. Bronze/Silver/Gold(Medallion Architecture)
- **Bronze:- raw JSON/CSV and ingestion metadata.
- **Silver:** parsed, cleaned, joined, validated, deduplicated, and appropriately protected applicant profile.
- **Gold:** underwriting decisions, reason codes, aggregates, and BI/HMDA reporting outputs.

 12. DBT  Project Structure
Separate source definitions and staging models from intermediate business logic and marts. Use views for lightweight staging when appropriate, tables for stable outputs, and incremental models for large datasets where only new/changed rows need processing. Add schema documentation and data tests.

 13. DBT  Tests
Apply `not_null` to required keys, `unique` where uniqueness is guaranteed, `relationships` for expected references, accepted-value tests for decision codes, source freshness tests, and custom tests for LTV/DTI thresholds and technical-rejection handling. Test business outcomes as well as column-level validity.

 14. Schema drift
Retain raw payloads in Bronze, compare incoming fields with expected schemas, and alert on missing, additional, or incompatible fields. Allow compatible additions only through controlled evolution. Quarantine or fail incompatible records rather than silently breaking downstream models.

 15. Incremental processing
Use a trustworthy source update timestamp, event time, or ingestion watermark. Configure DBT incremental models with a stable `unique_key` and a suitable merge strategy. Account for late-arriving updates and make reruns idempotent to avoid duplicate decisions.

 16. PII masking and RBAC
Use Snowflake Dynamic Data Masking and least-privilege RBAC. General analysts see masked SSNs and applicant names; authorized compliance roles can access cleartext only where required. Apply policies at the column level, restrict raw-table access, audit access, and avoid exposing unnecessary PII in Gold.

 17. Masking versus encryption
Masking controls what a user sees at query time. Encryption protects stored or transmitted data from unauthorized reading without the relevant key. They are complementary controls and neither replaces appropriate access grants and audit logging.

 18. HMDA reporting
Build structured Gold models with required application/loan attributes, dates, amounts, property information, action taken, and applicable reporting fields. Reconcile source counts and decisions, validate required fields, and keep reason codes. Confirm the exact HMDA mapping and reporting requirements with the compliance owner; the case-study fields alone may not satisfy every requirement.

 19. Secure Data Sharing
Expose only approved Gold views through Snowflake Secure Data Sharing, grant read-only access, minimize PII, and monitor access. Sharing can provide access without copying the underlying data. Any identifiable information must be justified and formally authorized.
1. Designing a Snowflake Secure Data Share for Gold Regulatory Reporting Data
Goal: Share the Gold regulatory reporting data with external federal auditors securely without physically moving or copying the data.

How Snowflake Secure Data Sharing works:

Data resides in your Snowflake account (the "provider").
You create a secure share object that exposes certain databases, schemas, tables, or views.
The consumer (external auditors) can query this data live from their own Snowflake account without data duplication.
Sharing is controlled and can be revoked anytime.
No data is copied or moved; it's accessed in real-time.
Steps to design:

Prepare the Gold Data for Sharing:

Create a dedicated schema (e.g., GOLD_REPORTING).
Put all regulatory reporting tables/views in that schema.
Ensure data is properly anonymized or masked if needed, as per compliance requirements.
Use views to provide only the necessary columns, and exclude sensitive or irrelevant fields.
Create the Secure Share:

In your Snowflake account, create a share, e.g. GOLD_DATA_SHARE.
Add the GOLD_REPORTING schema (or specific tables/views) to the share.
Grants: Ensure only SELECT privileges are given, no write access.
Restrict access to only the federal auditors' Snowflake accounts (identified by their account URL/ID).
Grant Access:

Share the Snowflake Account Name and share name with auditors.
They then create a database from your share on their Snowflake account.
Auditors query data live without you giving your raw data or copying it.
Audit & Security Controls:

Enable Snowflake’s Access History to monitor who accessed what.
Set up network policies or IP whitelisting if possible.
Use masking policies on columns like PII or sensitive financial info.
Example Conceptual Flow:



Your Account                  Auditors' Account
[ GOLD_REPORTING tables ]      
        |
        |  CREATE SHARE GOLD_DATA_SHARE; ADD SCHEMA GOLD_REPORTING;
        |
  ------------------->  Share Available to Federal Auditors
                             |
                  CREATE DATABASE FROM SHARE GOLD_DATA_SHARE;
                             |
                    Live querying Gold data without copying
2. Creating an Interactive Dashboard with Key Loan Metrics
You want an interactive dashboard with these elements:

Average FICO score of approved loans
Total loan volume approved vs. declined
Denial distribution by reason code
You can create this using Streamlit or Tableau. Since Tableau is widely used in enterprises and good for interactive dashboards, here’s how to conceptualize it:

Dashboard Design:
Data needed:

Column	Description
Loan_ID	Unique loan identifier
Loan_Status	Approved / Declined
FICO_Score	Numeric FICO score
Loan_Amount	Amount requested/approved
Denial_Reason_Code	Code explaining denial
Key Visualizations:

Average FICO Score of Approved Loans:

Filter loan data where Loan_Status = Approved.
Calculate the average FICO score and display it as a KPI card or Gauge.
Total Loan Volume Approved vs. Declined:

Aggregate loan amounts by loan status.
Show a side-by-side bar chart or pie chart comparing approved and declined volumes.
Denial Distribution by Reason Code:

Filter only declined loans.
Count loans per denial reason code.
Show a bar chart or donut chart of the counts.
Interactivity ideas:

Filters: Date ranges, loan types, states, or other dimensions.
Drill down on denial reasons for more detailed analysis.
Hover tooltips for additional context.


 21. Airflow orchestration
Create tasks for file/source readiness, application load, credit load, DBT Silver, DBT Gold, tests, and reporting. Set both source-load tasks as upstream dependencies of Silver. Configure retries with backoff, execution timeouts, logging, and failure alerts.

 22. Delayed credit bureau API
Allow application data to land in Bronze independently. Airflow waits on a sensor/readiness check for the bureau data. Alert if the delay exceeds the agreed SLA. When the data arrives, match by applicant ID, deduplicate safely, and resume processing. Do not make a final underwriting decision without required credit data.

 23. DBT  Model Failure
Capture logs and error details, retry transient failures, alert after retries are exhausted, and prevent downstream tasks from running. Correct the cause, rerun the relevant tasks, and verify idempotency and data consistency.

 24. Time Travel audit from 30 days ago
Check the table's configured retention first. If the relevant historical version is still retained, query with `AT (TIMESTAMP => ...)` using the exact approval timestamp and timezone. If retention has expired, use a saved audit snapshot or decision event if available. Do not assume Time Travel retains 30 days unless configured and supported for the table.

 25. 1,000x end-of-quarter spike
Use scalable object storage and Snowpipe/appropriate ingestion, tune Snowflake warehouse sizing and concurrency, process incrementally, and monitor queueing, lag, cost, and failures. Load test peak volumes. Make ingestion and transformation idempotent and ensure source joins and regulatory outputs remain correct under parallel load.

 26. Terraform
Use Terraform to provision supported Snowflake databases, schemas, warehouses, roles, and grants. Keep dev/test/prod separated, use version control and pull-request review, run `terraform plan` before apply, and manage credentials through an approved secrets mechanism rather than hardcoding them.

 27. Monitoring
Track file arrival, ingestion lag, row counts, rejected rows, schema changes, DBT tests, Airflow status, query duration, warehouse usage, decision volumes, average FICO of approved loans, and decline reasons. Alert on failed tasks, stale sources, unusual rejection rates, and cost/performance thresholds.

 28. CI/CD
Store DBT and Terraform code in Git. Pull requests trigger linting, compilation, tests, and infrastructure plans. After approval, deploy to the correct environment, run post-deployment checks, and restrict production privileges.

 29. Dashboard
Build a Streamlit or Tableau dashboard showing average FICO for approved loans, approved versus declined application counts/volume, decline reason distribution, approval trends, technical rejections, and data freshness. Source metrics from Gold tables and avoid exposing unnecessary PII.

 30. Slow DBT model
Inspect the Snowflake query profile for large scans, expensive joins, skew, spills, and queuing. Check filters and incremental logic, review warehouse size/concurrency, and compare runtime, cost, and result correctness after optimization.
Data Engineering Case Study — Mortgage Application & Underwriting Pipeline.
