-- Flink SQL Client:
-- docker exec -it lakehouse-lab-flink-jobmanager-1 ./bin/sql-client.sh

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3a://lake/paimon'
);

USE CATALOG paimon;
SET 'table.dml-sync' = 'true';
SET 'execution.runtime-mode' = 'BATCH';
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dm;

-- ODS transactions (upsert PK)
CREATE TABLE IF NOT EXISTS ods_transactions (
  tx_id STRING,
  event_time TIMESTAMP(3),
  merchant_id STRING,
  user_id STRING,
  amount DECIMAL(18,2),
  currency STRING,
  status STRING,
  op STRING,
  PRIMARY KEY (tx_id) NOT ENFORCED
) WITH (
  'bucket' = '2'
);

-- ODS merchants
CREATE TABLE IF NOT EXISTS ods_merchants (
  merchant_id STRING,
  event_time TIMESTAMP(3),
  mcc STRING,
  country STRING,
  is_active BOOLEAN,
  op STRING,
  PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
  'bucket' = '2'
);

-- DM
CREATE TABLE IF NOT EXISTS dm_daily_revenue_by_merchant (
  dt DATE,
  merchant_id STRING,
  country STRING,
  mcc STRING,
  gross_amount DECIMAL(18,2),
  tx_cnt BIGINT,
  last_update_ts TIMESTAMP(3),
  PRIMARY KEY (dt, merchant_id) NOT ENFORCED
) WITH (
  'bucket' = '2'
);
