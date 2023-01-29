from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.exceptions import AirflowException
import requests
import json


def check_table_exists(conn_hook, table_name):
    query = f"EXISTS TABLE {table_name}"
    records = conn_hook.get_records(query)
    return records[0][0]


def extract_load(**kwargs):
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')

    # Request to detection latest date
    api_request_latest = kwargs["api_request_latest"].format(api_symbols=kwargs["api_symbols"])
    response_latest = requests.get(api_request_latest)

    # Table name to insertion raw json files as json strings
    json_raw_tbl = kwargs["params"]["json_raw"]
    print(f"Table to import raw jsons: {json_raw_tbl}")

    if response_latest.status_code == 200:
        data_latest = response_latest.json()

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
            api_request_historical = kwargs["api_request_historical"]\
                .format(start_date=kwargs["api_historical_start_date"],
                        end_date=data_latest['date'],
                        symbols=kwargs["api_symbols"])

        # Main request
        print(f"api_request: {api_request_historical}")
        response = requests.get(api_request_historical)

        if response.status_code == 200:
            data = response.json()
            data_json_str = json.dumps(data)
            insert_sql_raw = kwargs["templates_dict"]["query_insert_sql_raw"]
            insert_sql_raw = insert_sql_raw.format(data_json_str=data_json_str)
            ch_hook.run(insert_sql_raw)
        else:
            raise AirflowException(
                f"Report for request {api_request_historical} is bad and status_code: {response.status_code}")

    else:
        raise AirflowException(
            f"Report for request {api_request_latest} is bad and status_code: {response_latest.status_code}")


def transform(**kwargs):
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    create_sql_target = kwargs["templates_dict"]["query_create_sql_target"]
    transform_sql = kwargs["templates_dict"]["query_transform_sql"]
    ch_hook.run(create_sql_target)
    ch_hook.run(transform_sql)
