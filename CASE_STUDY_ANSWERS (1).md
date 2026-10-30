CASE STUDY SOLUTION ﻿along with End-to-end architecture for Mortgage Application & Underwriting Pipeline case study, using Snowflake, DBT, Apache Airflow, and Terraform.

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

 20. Airflow orchestration
Create tasks for file/source readiness, application load, credit load, DBT Silver, DBT Gold, tests, and reporting. Set both source-load tasks as upstream dependencies of Silver. Configure retries with backoff, execution timeouts, logging, and failure alerts.

 21. Delayed credit bureau API
Allow application data to land in Bronze independently. Airflow waits on a sensor/readiness check for the bureau data. Alert if the delay exceeds the agreed SLA. When the data arrives, match by applicant ID, deduplicate safely, and resume processing. Do not make a final underwriting decision without required credit data.

 22. DBT  Model Failure
Capture logs and error details, retry transient failures, alert after retries are exhausted, and prevent downstream tasks from running. Correct the cause, rerun the relevant tasks, and verify idempotency and data consistency.

 23. Time Travel audit from 30 days ago
Check the table's configured retention first. If the relevant historical version is still retained, query with `AT (TIMESTAMP => ...)` using the exact approval timestamp and timezone. If retention has expired, use a saved audit snapshot or decision event if available. Do not assume Time Travel retains 30 days unless configured and supported for the table.

 24. 1,000x end-of-quarter spike
Use scalable object storage and Snowpipe/appropriate ingestion, tune Snowflake warehouse sizing and concurrency, process incrementally, and monitor queueing, lag, cost, and failures. Load test peak volumes. Make ingestion and transformation idempotent and ensure source joins and regulatory outputs remain correct under parallel load.

 25. Terraform
Use Terraform to provision supported Snowflake databases, schemas, warehouses, roles, and grants. Keep dev/test/prod separated, use version control and pull-request review, run `terraform plan` before apply, and manage credentials through an approved secrets mechanism rather than hardcoding them.

 26. Monitoring
Track file arrival, ingestion lag, row counts, rejected rows, schema changes, DBT tests, Airflow status, query duration, warehouse usage, decision volumes, average FICO of approved loans, and decline reasons. Alert on failed tasks, stale sources, unusual rejection rates, and cost/performance thresholds.

 27. CI/CD
Store DBT and Terraform code in Git. Pull requests trigger linting, compilation, tests, and infrastructure plans. After approval, deploy to the correct environment, run post-deployment checks, and restrict production privileges.

 28. Dashboard
Build a Streamlit or Tableau dashboard showing average FICO for approved loans, approved versus declined application counts/volume, decline reason distribution, approval trends, technical rejections, and data freshness. Source metrics from Gold tables and avoid exposing unnecessary PII.

 29. Slow DBT model
Inspect the Snowflake query profile for large scans, expensive joins, skew, spills, and queuing. Check filters and incremental logic, review warehouse size/concurrency, and compare runtime, cost, and result correctness after optimization.
Data Engineering Case Study — Mortgage Application & Underwriting Pipeline.
