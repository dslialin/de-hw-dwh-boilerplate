-- Простая стратегия: пересчитываем последние 2 дня (покрывает late events без водермарков)
WITH recent_tx AS (
  SELECT *
  FROM lake.ods.transactions
  WHERE event_time >= (current_timestamp() - INTERVAL 2 DAYS)
),
dim_merchants AS (
  SELECT * FROM lake.ods.merchants
),
agg AS (
  SELECT
    to_date(t.event_time) AS dt,
    t.merchant_id,
    m.country,
    m.mcc,
    cast(sum(CASE WHEN t.status = 'APPROVED' THEN t.amount ELSE 0 END) as decimal(18,2)) AS gross_amount,
    count(*) AS tx_cnt,
    current_timestamp() AS last_update_ts
  FROM recent_tx t
  LEFT JOIN dim_merchants m ON m.merchant_id = t.merchant_id
  GROUP BY 1,2,3,4
)
MERGE INTO lake.dm.daily_revenue_by_merchant d
USING agg s
ON d.dt = s.dt AND d.merchant_id = s.merchant_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
