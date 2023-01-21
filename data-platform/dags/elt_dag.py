from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import os
import json


json_raw_tbl_name =   "default.json_raw"    # Table name for raw json files extracted from API https://exchangerate.host/#/
rate_pairs_tbl_name = "default.rate_pairs"  # Table name for result parsed data partitioned by YYYYMM for date column
api_historical_start_date = "2023-01-01"
api_symbols = "BTC,USD"


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


    class SQLTemplatedPythonOperator(PythonOperator):
        template_ext = ('.sql', '.cql')


    def check_table_exists(conn_hook, tablename):
        query = f"EXISTS TABLE {tablename}"
        records = conn_hook.get_records(query)
        return records[0][0]


    # [START extract_load_function]
    def extract_load(**kwargs):
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')


        # Request to detection latest date
        api_request_latest = kwargs["api_request_latest"].format(api_symbols=kwargs["api_symbols"])
        response_latest = requests.get(api_request_latest)

        if response_latest.status_code == 200:
            data_latest = response_latest.json()
        else:
            print(f"Responce for request {api_request_latest} is bad and status_code: {response_latest.status_code}")


        # Table name to insertion raw json files as json strings
        json_raw_tbl = kwargs["params"]["json_raw"]
        print(f"Table to import raw jsons: {json_raw_tbl}")


        if check_table_exists(ch_hook, json_raw_tbl):
            print(f"Table {json_raw_tbl} to insertion raw json files as json strings already exists")

            # Using same start_date and end_date in api request
            api_request_historical = kwargs["api_request_historical"].format(start_date=data_latest['date'],
                                                                             end_date=data_latest['date'],
                                                                             symbols=kwargs["api_symbols"])
        else:
            print(f"Table {json_raw_tbl} to insertion raw json files as json strings not exists")

            # Create table for json data
            create_sql_json_raw = kwargs["templates_dict"]["query_create_sql_json_raw"]
            ch_hook.run(create_sql_json_raw)

            # Using start_date from params to get historical data through request
            api_request_historical = kwargs["api_request_historical"].format(start_date=kwargs["api_historical_start_date"],
                                                                             end_date=data_latest['date'],
                                                                             symbols=kwargs["api_symbols"])


        # Main request
        print(f"api_request: {api_request_historical}")
        response = requests.get(api_request_historical)

        if response.status_code == 200:
            data = response.json()
            data_json_str = json.dumps(data)
            insert_sql_raw = kwargs["templates_dict"]["query_insert_sql_raw"]
            insert_sql_raw = insert_sql_raw.replace("data_json_str", data_json_str)
            ch_hook.run(insert_sql_raw)
        else:
            print(f"Responce for request {api_request_historical} is bad and status_code: {response.status_code}")

    # [END extract_load_function]


    # [START transform_function]
    def transform(**kwargs):
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        create_sql_target = kwargs["templates_dict"]["query_create_sql_target"]
        transform_sql = kwargs["templates_dict"]["query_transform_sql"]
        ch_hook.run(create_sql_target)
        ch_hook.run(transform_sql)

    # [END transform_function]


    # [START main_flow]
    extract_load_task = SQLTemplatedPythonOperator(
        task_id='extract_load',
        python_callable=extract_load,
        provide_context=True,
        templates_dict={"query_create_sql_json_raw": "create_sql_json_raw.sql",
                        "query_insert_sql_raw":      "insert_sql_raw.sql"},
        params={"json_raw": f"{json_raw_tbl_name}"},
        op_kwargs={"api_historical_start_date": f"{api_historical_start_date}",
                   "api_symbols": f"{api_symbols}",
                   "api_request_latest":     "https://api.exchangerate.host/latest?symbols={api_symbols}",
                   "api_request_historical": "https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}&symbols={symbols}"
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

    # [END main_flow]
