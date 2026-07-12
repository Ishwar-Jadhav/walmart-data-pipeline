-- dbt model: staging/stg_customers.sql
-- Purpose: Transform raw Bronze customers into Silver conformed dimension
-- Layer: Bronze → Silver (dedupe, clean, conform to business rules)
-- Materialization: incremental (only process changed records)

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        tags=['staging', 'customers'],
        incremental_strategy='merge',
        on_schema_change='fail',
        indexes=[
            {'columns': ['customer_id'], 'type': 'hash'},
            {'columns': ['email'], 'type': 'hash'}
        ]
    )
}}

WITH source_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country,
        _loaded_at,
        _updated_at,
        _operation,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY _updated_at DESC
        ) as rn
    FROM {{ source('bronze', 'customers') }}
    
    {% if execute and flags.full_refresh == false %}
        -- Incremental: only process new/updated records since last run
        WHERE _updated_at >= (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    -- Keep only the latest version of each customer (handle duplicates)
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country,
        _loaded_at,
        _updated_at,
        _operation
    FROM source_data
    WHERE rn = 1
),

cleaned_data AS (
    -- Apply business rules and data quality fixes
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,  -- Remove non-numeric chars
        UPPER(country) AS country,
        CAST(_loaded_at AS DATE) AS load_date,
        CAST(_updated_at AS DATE) AS update_date,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM deduplicated
    WHERE
        -- Apply data quality filters
        customer_id IS NOT NULL
        AND email IS NOT NULL
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL
        AND country IN ('US', 'CA', 'MX', 'BR', 'UK', 'DE', 'FR', 'JP', 'AU')
)

SELECT * FROM cleaned_data
