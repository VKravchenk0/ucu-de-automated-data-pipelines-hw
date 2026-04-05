```bash
# activate venv
pip install apache-airflow
AIRFLOW_HOME="$(pwd)/airflow" airflow standalone
AIRFLOW_HOME="$(pwd)/airflow" airflow dags reserialize
```


