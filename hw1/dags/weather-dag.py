import json
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow import DAG

CITIES = {
    "Lviv":      (49.8397, 24.0297),
    "Kyiv":      (50.4501, 30.5234),
    "Kharkiv":   (49.9935, 36.2304),
    "Odesa":     (46.4825, 30.7233),
    "Zhmerynka": (49.0391, 28.1086),
}
CITY_NAMES = list(CITIES.keys())


def _get_city_params(logical_date, **_):
    api_key = Variable.get("WEATHER_API_KEY")
    dt = int(logical_date.timestamp())
    return [
        {"appid": api_key, "lat": lat, "lon": lon, "dt": dt}
        for lat, lon in CITIES.values()
    ]


def _process_weather(ti, **_):
    info = ti.xcom_pull(task_ids="extract_data", map_indexes=ti.map_index)
    city = CITY_NAMES[ti.map_index]
    current = info["data"][0]
    return current["dt"], current["temp"], current["humidity"], current["clouds"], current["wind_speed"], city


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 4, 17),
    schedule="@daily",
    catchup=False,
    tags=["weather"],
) as dag:
    db_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
                timestamp  TIMESTAMP,
                temp       FLOAT,
                humidity   FLOAT,
                clouds     FLOAT,
                wind_speed FLOAT,
                city       TEXT
            );
            """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn_http",
        endpoint="data/3.0/onecall/timemachine",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": CITIES["Lviv"][0],
            "lon": CITIES["Lviv"][1],
            "dt": "{{ data_interval_start.int_timestamp }}",
        },
    )

    get_city_params = PythonOperator(
        task_id="get_city_params",
        python_callable=_get_city_params,
    )

    extract_data = HttpOperator.partial(
        task_id="extract_data",
        http_conn_id="weather_conn_http",
        endpoint="data/3.0/onecall/timemachine",
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True,
    ).expand(data=get_city_params.output)

    process_data = PythonOperator.partial(
        task_id="process_data",
        python_callable=_process_weather,
    ).expand(op_kwargs=get_city_params.output)

    inject_data = SQLExecuteQueryOperator.partial(
        task_id="inject_data",
        conn_id="weather_conn",
        sql="INSERT INTO measures (timestamp, temp, humidity, clouds, wind_speed, city) VALUES (?, ?, ?, ?, ?, ?)",
    ).expand(parameters=process_data.output)

    db_create >> check_api >> get_city_params >> extract_data >> process_data >> inject_data
