-- Входные файлы generator кладёт как JSON Lines в s3a://lake/incoming/transactions/*.jsonl и merchants/*.jsonl

CREATE OR REPLACE TEMP VIEW stg_transactions
USING json
OPTIONS (
  path "s3a://lake/incoming/transactions/"
);

CREATE OR REPLACE TEMP VIEW stg_merchants
USING json
OPTIONS (
  path "s3a://lake/incoming/merchants/"
);

-- MERGE transactions
MERGE INTO lake.ods.transactions t
USING (
  SELECT
    tx_id,
    cast(event_time as timestamp) as event_time,
    merchant_id,
    user_id,
    cast(amount as decimal(18,2)) as amount,
    currency,
    status,
    op,
    current_timestamp() as ingest_ts
  FROM stg_transactions
) s
ON t.tx_id = s.tx_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op IN ('U','I') THEN UPDATE SET *
WHEN NOT MATCHED AND s.op IN ('I','U') THEN INSERT *;

-- MERGE merchants
MERGE INTO lake.ods.merchants t
USING (
  SELECT
    merchant_id,
    cast(event_time as timestamp) as event_time,
    mcc,
    country,
    cast(is_active as boolean) as is_active,
    op,
    current_timestamp() as ingest_ts
  FROM stg_merchants
) s
ON t.merchant_id = s.merchant_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op IN ('U','I') THEN UPDATE SET *
WHEN NOT MATCHED AND s.op IN ('I','U') THEN INSERT *;
