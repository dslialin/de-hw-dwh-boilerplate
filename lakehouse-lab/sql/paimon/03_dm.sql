CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3a://lake/paimon'
);

USE CATALOG paimon;
SET 'table.dml-sync' = 'true';
SET 'execution.runtime-mode' = 'BATCH';
-- Самый простой вариант: periodic пересчёт за 2 дня (запускается вручную студентом)
INSERT INTO dm_daily_revenue_by_merchant
SELECT
  CAST(t.event_time AS DATE) AS dt,
  t.merchant_id,
  m.country,
  m.mcc,
  CAST(SUM(CASE WHEN t.op <> 'D' AND t.status = 'APPROVED' THEN t.amount ELSE 0 END) AS DECIMAL(18,2)) AS gross_amount,
  COUNT(*) AS tx_cnt,
  CURRENT_TIMESTAMP AS last_update_ts
FROM ods_transactions t
LEFT JOIN ods_merchants m ON m.merchant_id = t.merchant_id
WHERE t.event_time >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
GROUP BY CAST(t.event_time AS DATE), t.merchant_id, m.country, m.mcc;

