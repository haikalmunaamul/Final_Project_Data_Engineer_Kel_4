from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',   
    'depends_on_past': False,    
    'email': ['your.email@example.com'],   
    'email_on_failure': False,  
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'remove_columns',
    default_args=default_args,
    description='remove columns using Spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

remove_columns = SparkSubmitOperator(
    task_id='data',
    application='/home/vigo/spark_script/spark_new_columns.py',
    conn_id='vigospark',
    total_executor_cores=1,
    executor_memory='1g',
    executor_cores=1,
    num_executors=1,
    name='remove-columns-processing-with-spark-submit',
    verbose=True,
    dag=dag,
)

remove_columns