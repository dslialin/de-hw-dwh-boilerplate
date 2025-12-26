{{ config(materialized='table') }}

SELECT
    fraud_date,
    fraud_type,
    count() AS fraud_cnt
FROM {{ ref('cdm_fraud_events') }}
GROUP BY fraud_date, fraud_type
ORDER BY fraud_date, fraud_type