CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3a://lake/paimon'
);

USE CATALOG paimon;
SET 'table.dml-sync' = 'true';
SET 'execution.runtime-mode' = 'BATCH';
-- Источник: incoming JSON lines в S3 (generator пишет в lake/incoming/transactions/ и merchants/)
CREATE TEMPORARY TABLE src_transactions (
  tx_id STRING,
  event_time STRING,
  merchant_id STRING,
  user_id STRING,
  amount STRING,
  currency STRING,
  status STRING,
  op STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://lake/incoming/transactions/',
  'format' = 'json'
);

CREATE TEMPORARY TABLE src_merchants (
  merchant_id STRING,
  event_time STRING,
  mcc STRING,
  country STRING,
  is_active STRING,
  op STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://lake/incoming/merchants/',
  'format' = 'json'
);

-- Ingest (upsert). Удаления упрощаем: пишем как "delete flag" через op, а чистим на витрине фильтром
INSERT INTO ods_transactions
SELECT
  tx_id,
  TO_TIMESTAMP_LTZ(
    UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z''') * 1000,
    3
  ) AS event_time,
  merchant_id,
  user_id,
  CAST(amount AS DECIMAL(18,2)),
  currency,
  status,
  op
FROM src_transactions;

INSERT INTO ods_merchants
SELECT
  merchant_id,
  TO_TIMESTAMP_LTZ(
    UNIX_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss''Z''') * 1000,
    3
  ) AS event_time,
  mcc,
  country,
  CAST(is_active AS BOOLEAN),
  op
FROM src_merchants;
