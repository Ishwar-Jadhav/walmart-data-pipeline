-- dbt model: intermediate/int_customer_orders_summary.sql
-- Purpose: Aggregate customer order metrics at the customer level
-- Layer: Silver → Gold preparation (business logic)
-- Materialization: incremental table with summary statistics

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        tags=['intermediate', 'customers', 'orders'],
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

WITH customer_base AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        country,
        dbt_loaded_at
    FROM {{ ref('stg_customers') }}
),

orders_base AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_amount,
        item_count,
        status,
        dbt_loaded_at
    FROM {{ ref('stg_orders') }}
    WHERE status NOT IN ('CANCELLED', 'RETURNED')  -- Exclude non-completed orders
),

customer_order_metrics AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        
        -- Order frequency metrics
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT DATE(o.order_date)) AS distinct_order_days,
        
        -- Monetary metrics
        COALESCE(SUM(o.order_amount), 0) AS total_spent,
        COALESCE(AVG(o.order_amount), 0) AS avg_order_value,
        COALESCE(MAX(o.order_amount), 0) AS max_order_value,
        COALESCE(MIN(o.order_amount), 0) AS min_order_value,
        
        -- Quantity metrics
        COALESCE(SUM(o.item_count), 0) AS total_items_ordered,
        COALESCE(AVG(o.item_count), 0) AS avg_items_per_order,
        
        -- Recency metrics
        COALESCE(MAX(o.order_date), c.dbt_loaded_at) AS last_order_date,
        DATEDIFF(DAY, MAX(o.order_date), CURRENT_DATE()) AS days_since_last_order,
        
        -- Time-based cohorts
        CASE
            WHEN MAX(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'Active'
            WHEN MAX(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'At Risk'
            ELSE 'Inactive'
        END AS customer_status,
        
        -- Calculation timestamp
        CURRENT_TIMESTAMP() AS metric_calculated_at
        
    FROM customer_base c
    LEFT JOIN orders_base o ON c.customer_id = o.customer_id
    
    {% if execute and flags.full_refresh == false %}
        -- Incremental: only recalculate for customers with recent changes
        WHERE c.dbt_loaded_at >= (SELECT MAX(metric_calculated_at) FROM {{ this }})
           OR o.dbt_loaded_at >= (SELECT MAX(metric_calculated_at) FROM {{ this }})
    {% endif %}
    
    GROUP BY
        c.customer_id, c.first_name, c.last_name, c.email, c.country, c.dbt_loaded_at
)

SELECT * FROM customer_order_metrics
