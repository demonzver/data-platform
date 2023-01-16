from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json

with DAG(
    'elt_dag',
    default_args={'retries': 0},
    description='ELT DAG',
    schedule_interval='0 */3 * * *',
    start_date=datetime(2022, 1, 9),
    catchup=False,
    tags=['zdm'],
    template_searchpath=['/opt/airflow/sql_requests'],
) as dag:

    def run_code_from_sql_requests(conn_hook, filename):
        filedir = os.path.join(os.environ['AIRFLOW_HOME'], 'sql_requests')
        filepath = os.path.join(filedir, filename)

        with open(filepath, 'r') as sql_file:
            sql_code = sql_file.read()
            queries = sql_code.split(';')

        # Filter from empty query
        queries[:] = [x for x in queries if x]

        # Execute every command from the input file
        for query in queries:
            conn_hook.run(query)
        return queries

    # [START extract_function]
    def extract_load(**kwargs):
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        url = 'https://api.exchangerate.host/latest?symbols=BTC,USD'
        resp = requests.get(url)
        if resp.status_code == 200:
            res = str(resp.json())
            res = res.replace("'", '"')
            run_code_from_sql_requests(ch_hook, 'create_sql_json_raw.sql')
            insert_sql_raw = run_code_from_sql_requests(ch_hook, 'insert_sql_raw.sql')[0].split()[-1]
            ch_hook.run(f"""INSERT INTO {insert_sql_raw} VALUES(now(),'{res}')""")
        else:
            print(f"Responce status_code: {resp.status_code}")

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        run_code_from_sql_requests(ch_hook, 'create_sql_target.sql')
        run_code_from_sql_requests(ch_hook, 'transform_sql.sql')

    # [END transform_function]

    # [START main_flow]
    extract_load_task = PythonOperator(
        task_id='extract_load',
        python_callable=extract_load,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    extract_load_task >> transform_task

    # [END main_flow]
