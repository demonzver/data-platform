from airflow.operators.python_operator import PythonOperator


class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql', '.cql')
