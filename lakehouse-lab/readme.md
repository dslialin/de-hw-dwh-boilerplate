# Lakehouse/Streamhouse Lab (simple)

## Track A: Iceberg + Spark
1) Start:
   docker compose -f docker/compose-iceberg.yml up -d --build

2) Open spark container:
   docker exec -it docker-spark-1 bash

3) Start spark-sql with Iceberg + S3A packages:
   # 1) убедимся, что директории существуют и доступны на запись
mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars
chmod -R 777 /tmp/.ivy2

   # 2) запустим spark-sql
```
spark-sql \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp -Divy.home=/tmp/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp -Divy.home=/tmp/.ivy2" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.spark:spark-hadoop-cloud_2.12:3.5.7 \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=s3a://lake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=adminadmin
```

4) Run SQL:
   -- in spark-sql:
   SOURCE /opt/sql/iceberg/01_tables.sql;
   SOURCE /opt/sql/iceberg/02_ingest.sql;
   SOURCE /opt/sql/iceberg/03_dm.sql;

5) Checks:
   SELECT count(*) FROM lake.ods.transactions;
   SELECT count(*) FROM lake.ods.merchants;
   SELECT dt, sum(gross_amount), sum(tx_cnt) FROM lake.dm.daily_revenue_by_merchant GROUP BY dt ORDER BY dt DESC;

6) Write checks into separate files, that would be your final results to show, here are some tips:

Save CSV with query results to S3:
```
INSERT OVERWRITE DIRECTORY 's3a://lake/exports/dm_daily_revenue'
USING csv
OPTIONS (
  header 'true'
)
SELECT *
FROM lake.dm.daily_revenue_by_merchant;
```

Save CSV with query results to local file of a docker container:
```
INSERT OVERWRITE DIRECTORY '/tmp/export_dm'
USING csv
OPTIONS (
  header 'true'
)
SELECT *
FROM lake.dm.daily_revenue_by_merchant;
```

Download file from docker container (run from your local machine):
```
docker cp docker-spark-1:/tmp/export_dm ./export_dm
```

**Save files with query results from p.5 into your repository!**

## Track B: Paimon + Flink
1) Start:
   docker compose -f docker/compose-paimon.yml up -d --build

Check if started fine:

   docker compose -f docker/compose-paimon.yml exec flink-jobmanager \  bash -lc 'ls -lah /opt/flink/lib | grep -i paimon || true'
   > -rw-r--r-- 1 root  root   51M Dec 21 15:33 paimon.jar

2) Run SQL:
   docker compose -f docker/compose-paimon.yml exec flink-jobmanager ./bin/sql-client.sh -f /opt/sql/paimon/01_tables.sql
   docker compose -f docker/compose-paimon.yml exec flink-jobmanager ./bin/sql-client.sh -f /opt/sql/paimon/02_ingest.sql;
   docker compose -f docker/compose-paimon.yml exec flink-jobmanager ./bin/sql-client.sh -f /opt/sql/paimon/03_dm.sql;

3) Open Flink SQL client:
   docker exec -it docker-flink-jobmanager-1 ./bin/sql-client.sh

4) Checks:
   SELECT count(*) FROM ods_transactions;
   SELECT * FROM dm_daily_revenue_by_merchant ORDER BY dt DESC, merchant_id LIMIT 20;

5) Output to the file (on local machine):
   docker compose -f docker/compose-paimon.yml exec flink-jobmanager \
    ./bin/sql-client.sh -f /opt/sql/paimon/04_flush.sql; > dm_preview.txt
