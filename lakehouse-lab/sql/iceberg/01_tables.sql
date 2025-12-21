CREATE NAMESPACE IF NOT EXISTS lake.ods;
CREATE NAMESPACE IF NOT EXISTS lake.dm;

-- ODS: transactions (PK tx_id, будем делать MERGE)
CREATE TABLE IF NOT EXISTS lake.ods.transactions (
  tx_id        STRING,
  event_time   TIMESTAMP,
  merchant_id  STRING,
  user_id      STRING,
  amount       DECIMAL(18,2),
  currency     STRING,
  status       STRING,
  op           STRING,
  ingest_ts    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time));

-- ODS: merchants (PK merchant_id)
CREATE TABLE IF NOT EXISTS lake.ods.merchants (
  merchant_id  STRING,
  event_time   TIMESTAMP,
  mcc          STRING,
  country      STRING,
  is_active    BOOLEAN,
  op           STRING,
  ingest_ts    TIMESTAMP
)
USING iceberg;

-- DM: daily revenue by merchant (пересчитываем по датам)
CREATE TABLE IF NOT EXISTS lake.dm.daily_revenue_by_merchant (
  dt            DATE,
  merchant_id   STRING,
  country       STRING,
  mcc           STRING,
  gross_amount  DECIMAL(18,2),
  tx_cnt        BIGINT,
  last_update_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (dt);
