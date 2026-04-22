import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

CITIES = {
    "Lviv":      (49.8397, 24.0297),
    "Kyiv":      (50.4501, 30.5234),
    "Kharkiv":   (49.9935, 36.2304),
    "Odesa":     (46.4825, 30.7233),
    "Zhmerynka": (49.0391, 28.1086),
}

WIND_SPEED_THRESHOLD = float(
    Variable.get("WIND_SPEED_THRESHOLD_MPS", default_var=10.0)
)

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

INSERT_SQL = """
    INSERT INTO weather_measures (timestamp, temp, humidity, clouds, wind_speed, city)
    VALUES (%(timestamp)s, %(temp)s, %(humidity)s, %(clouds)s, %(wind_speed)s, %(city)s)
"""


def _process_weather(city, ti, **_):
    info = ti.xcom_pull(task_ids=f"city_{city}.extract")
    current = info["data"][0]
    return {
        "timestamp": datetime.utcfromtimestamp(current["dt"]).isoformat(),
        "temp": current["temp"],
        "humidity": current["humidity"],
        "clouds": current["clouds"],
        "wind_speed": current["wind_speed"],
        "city": city,
    }


def _check_wind_speed(city, ti, **_):
    data = ti.xcom_pull(task_ids=f"city_{city}.transform")
    if data["wind_speed"] > WIND_SPEED_THRESHOLD:
        return f"city_{city}.alert"
    return f"city_{city}.normal_load"


def _send_alert(city, ti, **_):
    data = ti.xcom_pull(task_ids=f"city_{city}.transform")
    print(
        f"ALERT: Wind speed in {city} is {data['wind_speed']} m/s, "
        f"exceeding threshold of {WIND_SPEED_THRESHOLD} m/s"
    )


def _load_to_db(city, ti, **_):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    data = ti.xcom_pull(task_ids=f"city_{city}.transform")
    hook = PostgresHook(postgres_conn_id="airflow_db")
    hook.run(INSERT_SQL, parameters=data)


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 4, 17),
    schedule="@daily",
    catchup=False,
    tags=["weather"],
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
) as dag:
    db_create = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="airflow_db",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_measures (
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
            "appid": "{{ var.value.WEATHER_API_KEY }}",
            "lat": CITIES["Lviv"][0],
            "lon": CITIES["Lviv"][1],
            "dt": "{{ data_interval_start.int_timestamp }}",
        },
    )

    city_groups = []
    for city_name, (lat, lon) in CITIES.items():
        with TaskGroup(group_id=f"city_{city_name}") as city_tg:
            extract = HttpOperator(
                task_id="extract",
                http_conn_id="weather_conn_http",
                endpoint="data/3.0/onecall/timemachine",
                method="GET",
                data={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "lat": lat,
                    "lon": lon,
                    "dt": "{{ data_interval_start.int_timestamp }}",
                },
                response_filter=lambda x: json.loads(x.text),
                log_response=True,
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=_process_weather,
                op_kwargs={"city": city_name},
            )

            branch = BranchPythonOperator(
                task_id="branch",
                python_callable=_check_wind_speed,
                op_kwargs={"city": city_name},
            )

            normal_load = PythonOperator(
                task_id="normal_load",
                python_callable=_load_to_db,
                op_kwargs={"city": city_name},
            )

            alert = PythonOperator(
                task_id="alert",
                python_callable=_send_alert,
                op_kwargs={"city": city_name},
            )

            alert_load = PythonOperator(
                task_id="alert_load",
                python_callable=_load_to_db,
                op_kwargs={"city": city_name},
            )

            extract >> transform >> branch >> [normal_load, alert]
            alert >> alert_load

        city_groups.append(city_tg)

    db_create >> check_api >> city_groups
