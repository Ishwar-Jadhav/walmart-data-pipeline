-- dbt model: marts/fct_orders.sql
-- Purpose: Analytics-ready fact table for order transactions
-- Layer: Gold (fully denormalized, business-ready)
-- Materialization: incremental fact table with conformed dimensions

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        tags=['marts', 'fact', 'orders'],
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

WITH orders_base AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_amount,
        tax_amount,
        shipping_cost,
        discount_amount,
        item_count,
        status,
        dbt_loaded_at
    FROM {{ ref('stg_orders') }}
    
    {% if execute and flags.full_refresh == false %}
        -- Incremental: only process new/updated orders
        WHERE dbt_loaded_at >= (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),

customers_dim AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        country
    FROM {{ ref('dim_customers') }}
),

products_dim AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        unit_price
    FROM {{ ref('dim_products') }}
),

order_items_detail AS (
    -- Join order items to get product details
    SELECT
        oi.order_id,
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN products_dim p ON oi.product_id = p.product_id
),

order_items_aggregated AS (
    -- Aggregate product details per order
    SELECT
        order_id,
        COUNT(DISTINCT product_id) AS distinct_products,
        COLLECT_LIST(product_name) AS product_names,
        COLLECT_LIST(category) AS product_categories
    FROM order_items_detail
    GROUP BY order_id
),

final_orders AS (
    SELECT
        -- Keys
        o.order_id,
        o.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country AS customer_country,
        
        -- Order core metrics
        o.order_date,
        YEAR(o.order_date) AS order_year,
        MONTH(o.order_date) AS order_month,
        QUARTER(o.order_date) AS order_quarter,
        DAYOFWEEK(o.order_date) AS order_day_of_week,
        
        -- Financial metrics
        o.order_amount AS gross_order_amount,
        o.tax_amount,
        o.shipping_cost,
        o.discount_amount,
        o.order_amount - o.discount_amount AS net_order_amount,
        (o.order_amount - o.discount_amount) + o.tax_amount + o.shipping_cost AS total_order_value,
        
        -- Item metrics
        o.item_count,
        oa.distinct_products,
        
        -- Product details (denormalized for analytics)
        oa.product_names,
        oa.product_categories,
        
        -- Status tracking
        o.status,
        CASE
            WHEN o.status = 'COMPLETED' THEN 1
            ELSE 0
        END AS is_completed_flag,
        
        -- Derived metrics
        ROUND(CAST(o.order_amount AS DECIMAL(10, 2)) / NULLIF(o.item_count, 0), 2) AS avg_item_price,
        ROUND(CAST(o.discount_amount AS DECIMAL(10, 2)) / NULLIF(o.order_amount, 0), 4) AS discount_rate,
        
        -- Row number for ordering within customer
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id 
            ORDER BY o.order_date
        ) AS customer_order_sequence,
        
        -- Cumulative metrics (for cohort analysis)
        SUM(o.order_amount) OVER (
            PARTITION BY o.customer_id 
            ORDER BY o.order_date
        ) AS customer_cumulative_spend,
        
        -- Data pipeline metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at,
        '{{ run_started_at }}' AS dbt_run_timestamp
        
    FROM orders_base o
    LEFT JOIN customers_dim c ON o.customer_id = c.customer_id
    LEFT JOIN order_items_aggregated oa ON o.order_id = oa.order_id
)

SELECT * FROM final_orders
