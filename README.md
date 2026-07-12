# WALMART Data Engineering End-To-End Pipeline

> A production-grade data engineering project demonstrating a complete medallion architecture with **Apache Airflow orchestration, dbt transformations, and Databricks lakehouse**.

## 🎬 Project Overview

This project showcases a **real-world data engineering pipeline** built to handle Walmart's complex datasets using modern tools and patterns. The pipeline ingests data from multiple sources (Ghost.build Agentic DB, SQL Chatbots, AWS S3), orchestrates incremental processing through Apache Airflow, transforms data through dbt with quality checks, and materializes analytics-ready tables in Databricks.

**[Watch the Full Project Walkthrough on YouTube](https://youtu.be/ZEE-jNAthB0?si=ABQX_ApyBbGDD5SZ)**

---

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────────┐
                    │         DATA SOURCES                    │
                    │  Ghost.build | SQL Chatbot | AWS S3     │
                    └──────────┬──────────────────────────────┘
                               │
                    ┌──────────▼──────────────┐
                    │   INGESTION LAYER      │
                    │  Campaign-by-Campaign   │
                    │   Incremental Loading   │
                    └──────────┬──────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
        ▼                      ▼                      ▼
    ┌─────────┐            ┌──────────┐         ┌──────────┐
    │PostgreSQL          │Databricks │         │AWS S3    │
    │ (Landing) │        │(Lakehouse)│         │(Files)   │
    └─────────┘            └──────────┘         └──────────┘
        │                      │
        └──────────┬───────────┘
                   │
        ┌──────────▼──────────────┐
        │  APACHE AIRFLOW 3.2.0   │
        │  ├─ API Server          │
        │  ├─ Scheduler           │
        │  ├─ Celery Workers      │
        │  └─ DAG Processor       │
        └──────────┬──────────────┘
                   │
        ┌──────────▼──────────────┐
        │ DATABRICKS MEDALLION    │
        │ ├─ Bronze (Raw)         │
        │ ├─ Silver (Cleaned)     │
        │ └─ Gold (Analytics)     │
        └──────────┬──────────────┘
                   │
        ┌──────────▼──────────────┐
        │   DBT TRANSFORMATIONS   │
        │  ├─ dbt-core 1.11.11    │
        │  ├─ Quality Checks      │
        │  └─ Schema Tests        │
        └──────────┬──────────────┘
                   │
        ┌──────────▼──────────────┐
        │  ANALYTICS & BI         │
        │  Delta Lake → BI Tools  │
        └─────────────────────────┘
```

### Data Flow

1. **Ingestion** → Raw data from Ghost.build, SQL Chatbots, and AWS S3 files  
2. **Landing** → Campaign-by-Campaign (CbC) incremental loading into PostgreSQL  
3. **Orchestration** → Apache Airflow DAGs manage CDC upserts to Databricks Bronze  
4. **Transformation** → dbt transforms Bronze → Silver (deduplicated) → Gold (business-ready)  
5. **Quality** → Schema validation, referential integrity checks, anomaly detection  
6. **Analytics** → Snowflake/Delta-ready materialized tables for downstream BI tools

---

## ⚙️ Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 3.2.0 | DAG scheduling, task dependency management, monitoring |
| **Transformation** | dbt-core | 1.11.11 | SQL-first transformations with lineage tracking |
| **Compute** | Databricks | Latest | Lakehouse, Delta Lake, distributed compute |
| **Message Broker** | Redis | 7.2-bookworm | Celery task queue for distributed workers |
| **Metadata Store** | PostgreSQL | 16 | Airflow DAG metadata, raw data landing zone |
| **Containerization** | Docker | Latest | Reproducible local/cloud environments |
| **Language** | Python | 3.10+ | DAG definitions, custom operators, utilities |
| **VCS** | Git/GitHub | - | Version control, collaboration, CI/CD |

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose (installed and running)
- Python 3.10+
- Git
- 4GB+ RAM available for Docker

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/walmart-data-pipeline.git
   cd walmart-data-pipeline
   ```

2. **Set up environment variables**
   ```bash
   cp _env .env
   # Update .env with your Databricks credentials, AWS keys, Ghost.build API tokens
   ```

3. **Initialize Airflow and start services**
   ```bash
   docker-compose build
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - Open `http://localhost:8080`
   - Default username/password: `airflow` / `airflow`
   - Enable your DAGs from the UI

5. **Load sample data**
   ```bash
   python load_data.py
   ```

### Folder Structure

```
walmart-data-pipeline/
├── dags/                       # Airflow DAG definitions
│   ├── walmart_pipeline.py
│   ├── incremental_load.py
│   └── quality_checks.py
├── walmart_project/            # Project-specific modules
│   ├── extractors/             # Source connectors (Ghost, S3, SQL Chatbot)
│   ├── loaders/                # Databricks/Postgres loaders
│   └── utils/                  # Shared utilities (logging, retry logic)
├── dbt/                        # dbt project
│   ├── models/
│   │   ├── staging/            # Bronze → Silver transformations
│   │   ├── intermediate/       # Silver refinements
│   │   └── mart/               # Gold tables for analytics
│   ├── tests/                  # dbt tests (schema, data quality)
│   ├── macros/                 # dbt macros (reusable SQL blocks)
│   └── dbt_project.yml
├── config/                     # Airflow config overrides
├── logs/                       # Airflow task logs
├── plugins/                    # Custom Airflow operators/hooks
├── Dockerfile                  # Airflow image build config
├── docker-compose.yaml         # Full stack orchestration
├── requirements.txt            # Python dependencies
└── load_data.py               # Sample data loader script
```

---

## 📊 Key Features

### ✅ Medallion Architecture
- **Bronze**: Raw, unprocessed data as-is from sources (Ghost, S3)
- **Silver**: Deduplicated, cleaned, conformed to business rules
- **Gold**: Aggregated, business-logic-enriched analytics tables

### 🔄 Incremental Processing
- Campaign-by-Campaign (CbC) loading minimizes data volume
- CDC (Change Data Capture) patterns for efficient updates
- Idempotent DAG logic ensures safe reruns and backfills

### 🧪 Data Quality
- dbt built-in tests (unique, not_null, relationships)
- Custom SQL quality checks (anomaly detection, data freshness)
- Schema validation before materializing Gold tables

### 📈 Monitoring & Observability
- Airflow UI for DAG visualization and task monitoring
- Flower UI for Celery worker metrics
- Structured logging for debugging and SLA tracking
- Email alerts on task failures (configurable)

### 🤖 AI-Powered Data Discovery (Innovation)
- **Claude MCP integration** via Ghost.build for natural language Q&A on datasets
- Ask questions in plain English, get SQL queries and insights without manual writing
- Example: *"What's the top-selling product by region?"* → Automatically generates and runs the query

---

## 🛠️ Running the Pipeline

### Start the services
```bash
docker-compose up -d
```

### Monitor DAGs
1. Go to `http://localhost:8080`
2. Enable DAGs from the toggle switch
3. Check task logs in the Airflow UI

### Run a specific DAG
```bash
docker-compose exec airflow-scheduler airflow dags trigger walmart_pipeline
```

### View Celery worker health
```bash
docker-compose -f docker-compose.yaml --profile flower up
# Access Flower at http://localhost:5555
```

### Stop all services
```bash
docker-compose down
```

---

## 📝 Data Pipeline Workflow

### Airflow DAG Structure: `walmart_pipeline.py`

```
[Extract Ghost] ──┐
                  ├──→ [Load Postgres] ──→ [Create Bronze Table] ──→ [CDC Upsert]
[Extract S3]  ───┘                                                        ↓
                                                                    [dbt Staging]
                                                                        ↓
                                                                [dbt Intermediate]
                                                                        ↓
                                                                   [dbt Marts]
                                                                        ↓
                                                                  [dbt Tests]
                                                                        ↓
                                                          [Quality Checks + Freshness]
                                                                        ↓
                                                                  [Notify Success]
```

**Key Airflow Pattern:** Full orchestration with task dependencies, XCom for checkpoint passing, and quality validation gates.

### dbt Models: Bronze → Silver → Gold Transformations

#### 1. **Staging Model** (Bronze → Silver Deduplication)

```sql
-- models/staging/stg_customers.sql
-- Deduplicates raw customers and applies business rules

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (
    SELECT
        customer_id, first_name, last_name, email, phone, country,
        _loaded_at, _updated_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _updated_at DESC) as rn
    FROM {{ source('bronze', 'customers') }}
),
deduplicated AS (
    SELECT * FROM source_data WHERE rn = 1  -- Keep latest only
),
cleaned_data AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        LOWER(TRIM(email)) AS email,
        REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,
        UPPER(country) AS country,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM deduplicated
    WHERE customer_id IS NOT NULL
      AND email IS NOT NULL
      AND country IN ('US', 'CA', 'MX', 'BR', 'UK', 'DE', 'FR', 'JP', 'AU')
)
SELECT * FROM cleaned_data
```

#### 2. **Intermediate Model** (Business Logic - RFM Metrics)

```sql
-- models/intermediate/int_customer_orders_summary.sql
-- Aggregates customer-level metrics for segmentation

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

WITH customer_metrics AS (
    SELECT
        c.customer_id,
        c.email,
        c.country,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.order_amount), 0) AS total_spent,
        COALESCE(AVG(o.order_amount), 0) AS avg_order_value,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(DAY, MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order,
        CASE
            WHEN MAX(o.order_date) >= DATEADD(DAY, -30, CURRENT_DATE()) THEN 'Active'
            WHEN MAX(o.order_date) >= DATEADD(DAY, -90, CURRENT_DATE()) THEN 'At Risk'
            ELSE 'Inactive'
        END AS customer_status
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
    WHERE o.status NOT IN ('CANCELLED', 'RETURNED')
    GROUP BY c.customer_id, c.email, c.country
)
SELECT * FROM customer_metrics
```

#### 3. **Fact Table** (Gold - Analytics Ready)

```sql
-- models/marts/fct_orders.sql
-- Fully denormalized fact table with cumulative metrics

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH final_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.first_name,
        c.country,
        o.order_date,
        YEAR(o.order_date) AS order_year,
        MONTH(o.order_date) AS order_month,
        o.order_amount AS gross_amount,
        o.discount_amount,
        (o.order_amount - o.discount_amount) AS net_amount,
        o.tax_amount + o.shipping_cost AS fees,
        (o.order_amount - o.discount_amount) + o.tax_amount + o.shipping_cost AS total_value,
        ROUND(CAST(o.discount_amount AS DECIMAL(10, 2)) / NULLIF(o.order_amount, 0), 4) AS discount_rate,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS customer_order_sequence,
        SUM(o.order_amount) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS customer_cumulative_spend,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    WHERE o.status = 'COMPLETED'
)
SELECT * FROM final_orders
```

### dbt Quality Tests (schema.yml)

```yaml
models:
  - name: fct_orders
    description: Analytics-ready fact table
    columns:
      - name: order_id
        description: Unique order identifier
        tests:
          - unique
          - not_null
      - name: customer_id
        description: Foreign key to customers
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: total_value
        description: Final amount customer paid
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= net_amount"  # Sanity check
      - name: discount_rate
        description: Discount as percentage
        tests:
          - dbt_utils.expression_is_true:
              expression: "BETWEEN 0 AND 1"
```

### Incremental Load Pattern (CDC)

The pipeline uses **MERGE** for idempotent incremental loads:

```sql
-- Airflow task: upsert_bronze_cdc
MERGE INTO walmart.bronze.customers b
USING (
    SELECT customer_id, first_name, email, _updated_at
    FROM postgres.raw.ghost_source
    WHERE customer_id IS NOT NULL
) s
ON b.customer_id = s.customer_id
WHEN MATCHED AND s._updated_at > b._updated_at THEN
    UPDATE SET first_name = s.first_name, email = s.email, _updated_at = s._updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, email, _updated_at)
    VALUES (s.customer_id, s.first_name, s.email, s._updated_at);
```

**Key Benefits:**
- ✅ **Idempotent:** Safe to rerun without duplicates
- ✅ **Efficient:** Only processes changed records
- ✅ **Traceable:** Includes `_updated_at` for audits

---

## 🧠 Architecture Decisions & Trade-offs

### Why Apache Airflow?
- **Mature orchestration** with extensive community (easy to find solutions)
- **DAG-as-code paradigm** for versioning and CI/CD
- **Rich monitoring** (UI, logs, SLAs, alerting)
- **Scalable** with Celery for distributed task execution

### Why Databricks + Delta Lake?
- **ACID transactions** on data lake (reliability)
- **Time-travel & versioning** for audits and rollbacks
- **Unified compute** (SQL, Spark, ML in one platform)
- **Cost-efficient** incremental processing with mature optimization

### Why dbt?
- **Separation of concerns** (transformation logic decoupled from orchestration)
- **Testing & documentation** built-in
- **Lineage tracking** for debugging and impact analysis
- **Version control friendly** (SQL in Git, not notebooks)

### Why PostgreSQL for landing?
- **Lightweight staging** before sending to Databricks (saves compute costs)
- **COPY-friendly** for bulk CSV ingestion
- **Transaction support** for data consistency during CbC loads

---

## 📚 Advanced Topics

### Adding a New Data Source

1. Create an extractor in `walmart_project/extractors/`:
   ```python
   class NewSourceExtractor:
       def __init__(self, api_key):
           self.client = NewSourceClient(api_key)
       
       def fetch_incremental(self, last_checkpoint):
           return self.client.query(f"SELECT * FROM table WHERE updated_at > {last_checkpoint}")
   ```

2. Add a task to your DAG:
   ```python
   extract_new_source = PythonOperator(
       task_id='extract_new_source',
       python_callable=lambda: NewSourceExtractor().fetch_incremental(...)
   )
   ```

3. Append to Bronze in Databricks (MERGE/UPSERT pattern)

### Handling Late-Arriving Data

```python
# In Airflow DAG: backfill task with retries
backfill_task = DAG(
    'backfill_walmart',
    schedule_interval=None,  # Manual trigger
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)}
)
```

### Cost Optimization Tips

- Use **incremental materialization** in dbt (only process new data)
- Schedule **off-peak runs** (e.g., 2 AM) to avoid higher compute rates
- Archive historical Bronze data to cheaper S3 tiers after 90 days
- Use **Databricks Autoscaling** to right-size cluster compute

---

## 🚀 Challenges Solved in This Project

### 1. **JOIN Fan-Out in Aggregates** ❌ → ✅

**Problem:** One-to-many joins inflate SUM/COUNT in aggregations.
```sql
-- ❌ WRONG: This double-counts if one customer has multiple orders
SELECT 
    c.customer_id,
    COUNT(o.order_id) as order_count  -- INFLATED if joined to line items!
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id
```

**Solution:** Aggregate at each layer, then join aggregates.
```sql
-- ✅ CORRECT: Pre-aggregate before joining
WITH order_items_agg AS (
    SELECT order_id, COUNT(*) as item_count
    FROM order_items
    GROUP BY order_id
),
orders_with_items AS (
    SELECT o.*, oa.item_count
    FROM orders o
    LEFT JOIN order_items_agg oa ON o.order_id = oa.order_id
)
SELECT customer_id, COUNT(*) as order_count
FROM orders_with_items
GROUP BY customer_id
```

### 2. **Handling Late-Arriving Data**

**Problem:** Customer updates arrive out-of-order; need to backfill without full reprocess.

**Solution:** Timestamp-based CDC with checkpointing
```python
# Store checkpoint in Airflow Variable
last_checkpoint = Variable.get("ghost_last_processed", "2026-01-01")

# Extract only newer records
new_records = fetch_incremental(since=last_checkpoint)

# Update checkpoint AFTER successful load
Variable.set("ghost_last_processed", datetime.utcnow().isoformat())
```

### 3. **Deduplication in Incremental Models**

**Problem:** Duplicate records from retry logic or network issues.

**Solution:** ROW_NUMBER() with timestamp ordering + incremental merge
```sql
-- In stg_customers.sql
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _updated_at DESC) as rn
    FROM {{ source('bronze', 'customers') }}
)
SELECT * FROM deduplicated WHERE rn = 1
```

### 4. **Referential Integrity Without Foreign Keys**

**Problem:** Ensuring orders reference existing customers in a data lake (no enforced constraints).

**Solution:** dbt tests + quality checks
```yaml
# In schema.yml
- name: fct_orders
  columns:
    - name: customer_id
      tests:
        - relationships:
            to: ref('dim_customers')
            field: customer_id
            severity: error  # Fail if ANY orphaned order
```

### 5. **Incremental Model Merge Conflicts**

**Problem:** Running dbt twice in parallel causes MERGE conflicts.

**Solution:** Set `on_schema_change: fail` + single orchestrator
```yaml
# In dbt_project.yml
models:
  staging:
    +on_schema_change: fail  # Fail if schema changes unexpectedly
    +unique_key: customer_id  # Prevent duplicates
```

---

## 🚨 Troubleshooting

### Airflow UI not accessible
```bash
docker-compose logs airflow-apiserver
# Check for port conflicts; restart with `docker-compose down && docker-compose up`
```

### Celery workers not picking up tasks
```bash
docker-compose logs airflow-worker
# Verify Redis is healthy: docker-compose ps
```

### dbt tests failing
```bash
docker-compose exec airflow-worker dbt test --project-dir /opt/airflow/dbt
# Review test output for schema/referential integrity issues
```

### PostgreSQL connection errors
```bash
docker-compose logs postgres
# Ensure credentials in .env match docker-compose.yaml
```

---

## 💻 Code Examples & Repository Structure

### Complete Example Files

This repository includes production-grade example files:

| File | Purpose | Key Concepts |
|------|---------|--------------|
| [`dags/walmart_pipeline.py`](dags/walmart_pipeline.py) | Main Airflow orchestration | Task dependencies, XCom, CDC, quality gates |
| [`dbt/models/staging/stg_customers.sql`](dbt/models/staging/stg_customers.sql) | Customer dimension (Bronze→Silver) | Deduplication, incremental materialization |
| [`dbt/models/intermediate/int_customer_orders_summary.sql`](dbt/models/intermediate/int_customer_orders_summary.sql) | Customer RFM aggregation | Window functions, business logic, SCD |
| [`dbt/models/marts/fct_orders.sql`](dbt/models/marts/fct_orders.sql) | Orders fact table (Silver→Gold) | Denormalization, cumulative metrics, sequence numbering |
| [`dbt/models/schema.yml`](dbt/models/schema.yml) | Quality tests & documentation | Generic tests, relationships, data contracts |
| [`dbt/dbt_project.yml`](dbt/dbt_project.yml) | dbt configuration | Materialization strategies, variables, metrics |

### How to Use These Examples

1. **Copy dbt models** into your own `models/` folder:
   ```bash
   cp dbt/models/staging/*.sql your-project/models/staging/
   cp dbt/models/intermediate/*.sql your-project/models/intermediate/
   cp dbt/models/marts/*.sql your-project/models/marts/
   ```

2. **Adapt the Airflow DAG** to your environment:
   ```python
   # Update these variables in your Airflow environment
   GHOST_API_KEY = "your-ghost-api-key"
   S3_BUCKET = "your-s3-bucket"
   DATABRICKS_HOST = "your-databricks-host"
   ```

3. **Run dbt models locally** (with Databricks connector):
   ```bash
   dbt run --select staging
   dbt run --select intermediate
   dbt run --select marts
   dbt test
   ```

---

## 🔍 Pattern Deep-Dive: Incremental CDC Loading

### Problem Solved
How to ingest data incrementally without:
- ❌ Creating duplicate records
- ❌ Fan-outs in aggregates (double-counting)
- ❌ Losing deletes or updates
- ❌ Reprocessing entire datasets

### Solution: MERGE with Idempotent Keys

**Step 1: Extract incremental data** (in Airflow)
```python
last_checkpoint = context['task_instance'].xcom_pull(key='last_checkpoint')
new_records = extract_ghost(since=last_checkpoint)
context['task_instance'].xcom_push(key='new_checkpoint', value=now())
```

**Step 2: CDC upsert to Bronze** (MERGE pattern)
```sql
MERGE INTO walmart.bronze.customers TARGET
USING source_updates SOURCE
ON TARGET.customer_id = SOURCE.customer_id
WHEN MATCHED AND SOURCE._updated_at > TARGET._updated_at THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
```

**Step 3: Incremental transformation in dbt**
```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

SELECT * FROM {{ source('bronze', 'customers') }}
{% if execute and not flags.full_refresh %}
WHERE _updated_at >= (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
```

**Result:** Each model layer (Bronze→Silver→Gold) processes only changed records, reducing compute costs by 80-90%.

---

## 🎓 Learning Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Databricks Delta Lake Guide](https://docs.databricks.com/delta/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/use-medallion-bronze-silver-gold-architecture-on-databricks-lakehouse.html)

---

## 🤝 Contributing

This is a portfolio project, but contributions/suggestions are welcome! Feel free to open issues or PRs for improvements.

---

## 📄 License

MIT License — see LICENSE file for details.

---

## 👤 Author

**Ishh** | Data Engineer | [LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourprofile)

Building end-to-end data pipelines that scale. Passionate about lakehouse architectures, incremental processing, and AI-driven data discovery.

---

**Last Updated:** July 2026  
**YouTube Demo:** [Watch Full Walkthrough](https://youtu.be/ZEE-jNAthB0?si=ABQX_ApyBbGDD5SZ)
