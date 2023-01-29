from airflow import DAG
from operators.sql_templated_python_operator import SQLTemplatedPythonOperator
from elt.funcs import extract_load, transform
import pendulum


json_raw_tbl_name = "default.json_raw"  # Table name for raw json files extracted from API https://exchangerate.host/#/
rate_pairs_tbl_name = "default.rate_pairs"  # Table name for result parsed data partitioned by YYYY-MM for date column
api_historical_start_date = "2023-01-01"
api_symbols = "BTC,USD"
api_request_latest_url = "https://api.exchangerate.host/latest?symbols={api_symbols}"
api_request_historical_url = \
    """https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}&symbols={symbols}"""


with DAG(
    'elt_dag',
    default_args={'retries': 0},
    description='ELT DAG',
    schedule_interval='0 */3 * * *',
    start_date=pendulum.datetime(2022, 1, 9, tz="UTC"),
    catchup=False,
    tags=['zdm'],
    template_searchpath='/opt/airflow/sql_requests',
) as dag:

    extract_load_task = SQLTemplatedPythonOperator(
        task_id='extract_load',
        python_callable=extract_load,
        provide_context=True,
        templates_dict={"query_create_sql_json_raw": "create_sql_json_raw.sql",
                        "query_insert_sql_raw":      "insert_sql_raw.sql"},
        params={"json_raw": f"{json_raw_tbl_name}"},
        op_kwargs={"api_historical_start_date": f"{api_historical_start_date}",
                   "api_symbols": f"{api_symbols}",
                   "api_request_latest": f"{api_request_latest_url}",
                   "api_request_historical": f"{api_request_historical_url}"
                   },
    )

    transform_task = SQLTemplatedPythonOperator(
        task_id='transform',
        python_callable=transform,
        templates_dict={"query_create_sql_target": "create_sql_target.sql",
                        "query_transform_sql":     "transform_sql.sql"},
        params={"json_raw":   f"{json_raw_tbl_name}",
                "rate_pairs": f"{rate_pairs_tbl_name}"},
    )

    extract_load_task >> transform_task
