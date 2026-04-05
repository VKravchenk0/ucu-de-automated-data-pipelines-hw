import json
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow import DAG

def _process_weather(ti):
    info = ti.xcom_pull("extract_data")
    timestamp = info["dt"] 
    temp = info["main"]["temp"]
    return timestamp, temp


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 4, 5),
    schedule="@hourly",
    catchup=False,
    tags=["weather"],
) as dag:
    db_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS
            measures
            (timestamp TIMESTAMP, temp FLOAT);
            """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn_http",
        endpoint="data/2.5/weather",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), "q": "Lviv"}
    )

    extract_data = HttpOperator(
        task_id="extract_data",
        http_conn_id="weather_conn_http",
        endpoint="data/2.5/weather",
        data={"appid": Variable.get("WEATHER_API_KEY"), "q": "Lviv"},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_weather
    )

    inject_data = SQLExecuteQueryOperator(
        task_id="inject_data",
        conn_id="weather_conn",
        sql="""
            INSERT INTO measures (timestamp, temp) VALUES
            ({{ti.xcom_pull(task_ids='process_data')[0]}},
            {{ti.xcom_pull(task_ids='process_data')[1]}});
        """,
    )

    db_create >> check_api >> extract_data >> process_data >> inject_data