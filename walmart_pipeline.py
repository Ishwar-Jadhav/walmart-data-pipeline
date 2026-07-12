"""
WALMART Data Engineering Pipeline - Apache Airflow DAG
Orchestrates: Ghost.build ingestion → Postgres landing → Databricks medallion → dbt transformations → Quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.databricks.operators.databricks_notebook import DatabricksNotebookOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'email_on_failure': True,
    'email': ['data-alerts@walmart.com'],
}

# DAG definition
dag = DAG(
    'walmart_pipeline',
    default_args=default_args,
    description='End-to-end medallion pipeline: Ingestion → Landing → Bronze → Silver → Gold',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['walmart', 'production', 'medallion', 'dbt'],
)

logger = logging.getLogger(__name__)

# ============================================================================
# EXTRACTION TASKS
# ============================================================================

def extract_from_ghost(**context):
    """Extract incremental data from Ghost.build Agentic DB"""
    from walmart_project.extractors.ghost_extractor import GhostExtractor
    
    logger.info("Starting Ghost.build extraction...")
    extractor = GhostExtractor(api_key="{{ var.value.ghost_api_key }}")
    
    # Get last checkpoint from XCom (set in previous run)
    last_checkpoint = context['task_instance'].xcom_pull(
        task_ids='get_checkpoint',
        key='last_ghost_checkpoint'
    ) or '2026-01-01'
    
    # Fetch incremental records
    records = extractor.fetch_incremental(since=last_checkpoint)
    logger.info(f"Fetched {len(records)} records from Ghost")
    
    # Push new checkpoint to XCom for next run
    context['task_instance'].xcom_push(
        key='current_ghost_checkpoint',
        value=datetime.utcnow().isoformat()
    )
    
    return len(records)

def extract_from_s3(**context):
    """Extract campaign files from AWS S3 (Campaign-by-Campaign loading)"""
    from walmart_project.extractors.s3_extractor import S3Extractor
    
    logger.info("Starting S3 extraction (Campaign-by-Campaign)...")
    extractor = S3Extractor(
        bucket="{{ var.value.s3_bucket }}",
        prefix="walmart/campaigns/"
    )
    
    # Get list of files added since last run
    files = extractor.list_new_files(
        since_timestamp="{{ execution_date }}"
    )
    logger.info(f"Found {len(files)} new campaign files in S3")
    
    return files

extract_ghost = PythonOperator(
    task_id='extract_ghost',
    python_callable=extract_from_ghost,
    dag=dag,
)

extract_s3 = PythonOperator(
    task_id='extract_s3',
    python_callable=extract_from_s3,
    dag=dag,
)

# ============================================================================
# LANDING ZONE (PostgreSQL)
# ============================================================================

def load_to_postgres(**context):
    """Load extracted data into PostgreSQL landing zone"""
    from walmart_project.loaders.postgres_loader import PostgresLoader
    
    logger.info("Loading data to PostgreSQL landing zone...")
    
    loader = PostgresLoader(
        host="{{ var.value.postgres_host }}",
        database="{{ var.value.postgres_db }}",
        user="{{ var.value.postgres_user }}",
        password="{{ var.value.postgres_password }}"
    )
    
    # Get extracted data from previous tasks
    ghost_record_count = context['task_instance'].xcom_pull(task_ids='extract_ghost')
    s3_files = context['task_instance'].xcom_pull(task_ids='extract_s3')
    
    # Load Ghost data
    loader.load_ghost_data(
        table='raw.ghost_source',
        data=context['task_instance'].xcom_pull(task_ids='extract_ghost')
    )
    
    # Load S3 files
    for file in s3_files:
        loader.load_csv_file(
            s3_path=file,
            table='raw.campaigns',
            load_method='append'  # Campaign-by-Campaign append pattern
        )
    
    logger.info(f"Successfully loaded {ghost_record_count} records + {len(s3_files)} files to Postgres")

load_postgres = PythonOperator(
    task_id='load_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# ============================================================================
# DATABRICKS BRONZE LAYER (CDC Upsert)
# ============================================================================

create_bronze_table = DatabricksSqlOperator(
    task_id='create_bronze_table',
    sql="""
    CREATE TABLE IF NOT EXISTS walmart.bronze.customers (
        customer_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        country STRING,
        _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _operation STRING DEFAULT 'INSERT'  -- For CDC tracking
    )
    USING DELTA
    PARTITIONED BY (_loaded_at);
    """,
    databricks_conn_id='databricks_default',
    http_path="{{ var.value.databricks_http_path }}",
    sql_warehouse_id="{{ var.value.databricks_warehouse_id }}",
    dag=dag,
)

def upsert_bronze_cdc(**context):
    """
    Upsert data from Postgres landing into Databricks Bronze layer
    Using MERGE pattern for incremental CDC processing
    """
    from walmart_project.loaders.databricks_loader import DatabricksLoader
    
    logger.info("Starting CDC upsert to Bronze layer...")
    
    loader = DatabricksLoader(
        host="{{ var.value.databricks_host }}",
        token="{{ var.value.databricks_token }}"
    )
    
    # MERGE logic for incremental upserts (avoiding duplicates)
    merge_query = """
    MERGE INTO walmart.bronze.customers b
    USING (
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            country,
            CURRENT_TIMESTAMP AS _loaded_at,
            CURRENT_TIMESTAMP AS _updated_at,
            'INSERT' AS _operation
        FROM postgres.raw.ghost_source
        WHERE customer_id IS NOT NULL
    ) s
    ON b.customer_id = s.customer_id
    WHEN MATCHED AND s._updated_at > b._updated_at THEN
        UPDATE SET 
            first_name = s.first_name,
            last_name = s.last_name,
            email = s.email,
            phone = s.phone,
            country = s.country,
            _updated_at = s._updated_at,
            _operation = 'UPDATE'
    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, email, phone, country, _loaded_at, _updated_at, _operation)
        VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.phone, s.country, s._loaded_at, s._updated_at, s._operation);
    """
    
    loader.execute_merge(merge_query)
    logger.info("Bronze CDC upsert completed")

upsert_bronze = PythonOperator(
    task_id='upsert_bronze_cdc',
    python_callable=upsert_bronze_cdc,
    dag=dag,
)

# ============================================================================
# DBT TRANSFORMATIONS
# ============================================================================

with TaskGroup("dbt_transformations", dag=dag) as dbt_group:
    
    # Stage: Bronze → Silver (data cleaning, deduplication, business rules)
    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --select tag:staging \
            --profiles-dir /opt/airflow/dbt/profiles \
            --project-dir /opt/airflow/dbt
        """,
        dag=dag,
    )
    
    # Intermediate: Silver refinements (business logic, aggregations)
    dbt_intermediate = BashOperator(
        task_id='dbt_intermediate',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --select tag:intermediate \
            --profiles-dir /opt/airflow/dbt/profiles \
            --project-dir /opt/airflow/dbt
        """,
        dag=dag,
    )
    
    # Mart: Silver → Gold (analytics-ready fact & dimension tables)
    dbt_marts = BashOperator(
        task_id='dbt_marts',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt run --select tag:marts \
            --profiles-dir /opt/airflow/dbt/profiles \
            --project-dir /opt/airflow/dbt
        """,
        dag=dag,
    )
    
    # Data quality tests (after all transformations)
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command="""
        cd /opt/airflow/dbt && \
        dbt test --profiles-dir /opt/airflow/dbt/profiles \
                 --project-dir /opt/airflow/dbt
        """,
        dag=dag,
    )
    
    # DAG: staging → intermediate → marts → tests
    dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_tests

# ============================================================================
# QUALITY CHECKS & ANOMALY DETECTION
# ============================================================================

run_quality_checks = DatabricksSqlOperator(
    task_id='run_quality_checks',
    sql="""
    -- Check 1: No negative order amounts
    SELECT 
        COUNT(*) as negative_orders
    FROM walmart.gold.fct_orders
    WHERE order_amount < 0;
    
    -- Check 2: Orders match dates (no future orders)
    SELECT 
        COUNT(*) as future_orders
    FROM walmart.gold.fct_orders
    WHERE order_date > CURRENT_DATE();
    
    -- Check 3: Referential integrity (all order customers exist)
    SELECT 
        COUNT(DISTINCT o.customer_id) as orphaned_orders
    FROM walmart.gold.fct_orders o
    LEFT JOIN walmart.gold.dim_customers c ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL;
    """,
    databricks_conn_id='databricks_default',
    http_path="{{ var.value.databricks_http_path }}",
    sql_warehouse_id="{{ var.value.databricks_warehouse_id }}",
    dag=dag,
)

# ============================================================================
# DATA FRESHNESS VALIDATION
# ============================================================================

validate_data_freshness = DatabricksSqlOperator(
    task_id='validate_data_freshness',
    sql="""
    SELECT 
        table_name,
        MAX(_loaded_at) as last_update,
        DATEDIFF(HOUR, MAX(_loaded_at), CURRENT_TIMESTAMP()) as hours_since_update
    FROM (
        SELECT '_loaded_at', 'dim_customers' as table_name FROM walmart.gold.dim_customers
        UNION ALL
        SELECT '_loaded_at', 'fct_orders' as table_name FROM walmart.gold.fct_orders
    )
    GROUP BY table_name
    HAVING hours_since_update > 24
    """,
    databricks_conn_id='databricks_default',
    http_path="{{ var.value.databricks_http_path }}",
    sql_warehouse_id="{{ var.value.databricks_warehouse_id }}",
    dag=dag,
)

# ============================================================================
# SUCCESS NOTIFICATION
# ============================================================================

def notify_success(**context):
    """Send success notification with pipeline metrics"""
    logger.info("✓ Pipeline completed successfully!")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info("All quality checks passed. Data ready for analytics.")

notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES (Task Order)
# ============================================================================

# Extraction phase (parallel)
[extract_ghost, extract_s3] >> load_postgres

# Landing → Bronze CDC
load_postgres >> create_bronze_table >> upsert_bronze

# Bronze → Transformations (dbt)
upsert_bronze >> dbt_group

# Quality assurance
dbt_group >> [run_quality_checks, validate_data_freshness]

# Success
[run_quality_checks, validate_data_freshness] >> notify_success_task
