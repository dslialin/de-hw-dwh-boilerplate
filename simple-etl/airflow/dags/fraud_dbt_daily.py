from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


def map_to_prev_15m(logical_date, **_):
    dt = pendulum.instance(logical_date).in_timezone("UTC").replace(second=0, microsecond=0)
    m = (dt.minute // 15) * 15
    return dt.replace(minute=m)


with DAG(
    dag_id="fraud_dbt_daily",
    schedule="30 16 * * *",
    start_date=pendulum.datetime(2025, 12, 26, 16, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_ods = ExternalTaskSensor(
        task_id="wait_pg_to_ch_simple_dwh",
        external_dag_id="pg_to_ch_simple_dwh",
        external_task_id="load_transactions_and_fact",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        timeout=60 * 60,
        poke_interval=30,
        execution_date_fn=map_to_prev_15m,
    )

    run_dbt = BashOperator(
        task_id="dbt_run_cdm_dm",
        bash_command="cd /opt/airflow/dbt && dbt run --select cdm_fraud_events dm_fraud_daily_counts",
    )

    wait_ods >> run_dbt