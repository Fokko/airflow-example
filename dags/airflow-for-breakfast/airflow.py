import glob
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from os import path

from airflow import DAG

CURRENT_DIR = path.join(path.dirname(path.realpath(__file__)))


def clean_name(str):
    return str.split('/')[-1].split('.')[0].replace('-', '_')


default_args = {
    "owner": "Fokko Driesprong",
    "email": ["fokkodriesprong@godatadriven.com"],
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1)
}

with DAG(
        dag_id='airflow-for-breakfast-5',
        schedule_interval='@daily',
        start_date=datetime(2016, 1, 1),
        default_args=default_args,
        catchup=False
) as dag:
    init = BashOperator(
        task_id='say_hi',
        bash_command='echo "Hi $who at {{ ds }}" && rm -rf /data/{{ ds }}/ && mkdir -p /data/{{ ds }}/',
        env={
            'who': 'Airflow'
        },
        dag=dag
    )

    spark_analysis = SparkSubmitOperator(
        task_id='perform-spark-analysis',
        application=CURRENT_DIR + '/spark-analysis.py',
        dag=dag
    )

    for sql_file in glob.glob('{}/sql/*.sql'.format(CURRENT_DIR)):
        with open(sql_file, "r") as file:
            sql_query = file.read()
        sql_query = sql_query.replace('\n', ' ')

        table_name = clean_name(sql_file)

        postgres_export = PostgresOperator(
            task_id='import_{}'.format(table_name),
            sql=sql_query,
            dag=dag,
            pool='postgres'
        )

        init >> postgres_export >> spark_analysis
