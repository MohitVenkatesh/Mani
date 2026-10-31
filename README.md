# Data Engineering Case Study: Mortgage Application & Underwriting Pipeline

## 1. Project Overview

A leading national mortgage lender is modernizing its loan origination and underwriting systems.

The objective is to design and implement a secure, automated ELT pipeline that:

- Ingests mortgage application and credit bureau data.
- Applies automated underwriting business rules.
- Implements data quality and validation checks.
- Protects Personally Identifiable Information (PII).
- Structures data for HMDA regulatory compliance reporting.
- Supports business intelligence and regulatory dashboards.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Data Ingestion | Snowflake Native / Snowpipe |
| Data Warehouse | Snowflake |
| Data Transformation | dbt |
| Processing | Snowflake SQL |
| Orchestration | Apache Airflow / Prefect |
| Security | Dynamic Data Masking, RBAC |
| Infrastructure as Code | Terraform |
| Dashboard | Streamlit / Tableau |
| Architecture | Medallion Architecture |

## 3. Source Data

### Source 1: Mortgage Applications (JSON)

Contains:
- Applicant details
- Requested loan amount
- Property purchase price
- Monthly gross income

### Source 2: Credit Bureau Records (CSV)

Contains:
- Applicant Social Security Number (SSN)
- Credit Score (FICO)
- Existing monthly debt obligations

Both datasets are securely loaded into Snowflake Bronze landing tables.

## 4. Medallion Architecture

### Bronze Layer – Raw Data Ingestion

- Ingest mortgage application JSON files.
- Ingest credit bureau CSV files.
- Store raw source data in Snowflake.
- Maintain source metadata and ingestion timestamps.

### Silver Layer – Data Cleaning and Validation

- Join mortgage applications and credit bureau records using a unique applicant identifier.
- Implement PII masking for SSN and applicant name.
- Allow sensitive data visibility only to authorized compliance officers.
- Reject records with critical missing fields.
- Log rejected records into `TECHNICAL_REJECTIONS`.

### Gold Layer – Business Rules and Underwriting

Apply automated underwriting eligibility rules.

#### Loan-to-Value (LTV)

Formula:

LTV = (Loan Amount / Property Value) × 100

Business Rule:
- If LTV exceeds 95%, decline the loan.

#### Debt-to-Income (DTI)

Formula:

DTI = (Monthly Debt / Monthly Gross Income) × 100

Business Rule:
- If DTI exceeds 43%, decline the loan.

#### Credit Score Threshold

- Credit score below 620: Decline.
- Credit score between 620 and 660, with DTI between 36% and 43%: Requires Co-signer (Optional/Bonus).

#### Decision and Reason Logging

Store all underwriting decisions in a Gold compliance table.

Example reason codes:
- `DECLINED_DTI_EXCEEDED`
- `DECLINED_LTV_EXCEEDED`
- `DECLINED_CREDIT_SCORE`
- `REQUIRES_CO_SIGNER`
- `APPROVED`

## 5. Data Storage Design

| Layer | Description |
|---|---|
| Bronze | Raw JSON and CSV landing tables |
| Silver | Cleaned, joined, and masked master customer applications |
| Gold | Structured and aggregated tables optimized for HMDA regulatory reporting and BI dashboards |

## 6. Orchestration and Monitoring

Use Apache Airflow or Prefect to orchestrate the dbt pipeline.

Sequential dependencies:

1. Mortgage application data ingestion.
2. Credit bureau data ingestion.
3. Validate successful completion of both ingestion processes.
4. Execute dbt Silver transformation models.
5. Execute Gold underwriting models.
6. Run data quality tests.
7. Publish compliance reporting tables.

Implement automated alerts whenever an ingestion or dbt transformation stage fails.

## 7. Optional Enhancements

### Snowflake Secure Data Sharing

Design a Snowflake Secure Data Share to share Gold regulatory reporting data with external federal auditors without physically moving the data.

### Interactive Dashboard

Build a dashboard using Streamlit or Tableau showing:

- Average FICO score of approved loans.
- Total approved versus declined loan volume.
- Denial distribution by reason code.

## 8. Design Questions and Answers

### A. How would you handle delayed Credit Bureau API data?

Implement an asynchronous ingestion and dependency-based orchestration mechanism.

- Ingest mortgage application data independently.
- Store application records in the Bronze layer.
- Track credit bureau data arrival status.
- Maintain pending applications until credit bureau data arrives.
- Trigger the Silver transformation only after both datasets are successfully loaded.
- Use Airflow or Prefect sensors to monitor data availability.
- Implement retries, timeout handling, and automated alerts.

This ensures incomplete applications are not incorrectly processed by the underwriting engine.

### B. How would you use Snowflake Time Travel to audit an applicant's financial profile from 30 days ago?

Use Snowflake Time Travel to query historical table data as it existed at a particular point in time.

Example:

```sql
SELECT *
FROM GOLD_MORTGAGE_APPLICATIONS
AT (TIMESTAMP => '2026-10-01 10:00:00'::TIMESTAMP);
```

For auditing:
- Retrieve the historical application record.
- Verify the applicant's credit score, loan amount, property value, and monthly debt.
- Recalculate LTV and DTI.
- Compare the historical financial profile with the original underwriting decision.
- Maintain decision timestamps and reason codes for auditability.

Note: Snowflake Time Travel is subject to the configured retention period. A 30-day historical query requires sufficient retention or an appropriate historical snapshot/archive.

### C. How would you handle a 1,000x application volume spike during an end-of-quarter rush?

Design the architecture for horizontal scalability and workload isolation.

- Use Snowpipe or Snowpipe Streaming for scalable ingestion, depending on the source delivery pattern.
- Leverage Snowflake virtual warehouses with appropriate sizing and auto-scaling.
- Configure multi-cluster warehouses for concurrent workloads.
- Implement incremental dbt models instead of full refreshes wherever appropriate.
- Use micro-batching and partition-aware processing strategies.
- Separate ingestion, transformation, and reporting workloads.
- Monitor warehouse utilization, query performance, and processing latency.
- Implement retry mechanisms and workload prioritization.

## 9. Deliverables

1. Source code/scripts for data simulation, dbt models, and orchestration.
2. Terraform code to provision:
   - Snowflake databases
   - Schemas
   - Virtual warehouses
   - Role-based access controls (RBAC)
3. High-level architecture diagram demonstrating Bronze-Silver-Gold flow.
4. Documentation explaining LTV and DTI calculation logic.
5. Written design answers covering delayed data, historical auditing, and scalability.

## 10. Evaluation Criteria

- Correctness of LTV and DTI calculations.
- Accuracy of underwriting routing logic.
- Quality of dbt project structure.
- Appropriate model materialization choices.
- Data quality testing and validation.
- Security implementation and PII masking.
- Clarity of architectural documentation.
- Medallion architecture and design patterns.

---

## 11. Expected Project Outcome

A secure, scalable, automated mortgage underwriting data platform using Snowflake, dbt, Airflow/Prefect, and Terraform.

The platform should provide reliable data ingestion, validated financial information, automated loan eligibility decisions, PII protection, and regulatory reporting capabilities.
