from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract_salesorder(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgresvigoconn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM salesorder")
    salesorder_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    salesorder_df = pd.DataFrame(salesorder_data, columns=column_names)
    salesorder_df.to_csv('/tmp/salesorder.csv', index=False)

def extract_product(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgresvigoconn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM product")
    product_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    product_df = pd.DataFrame(product_data, columns=column_names)
    product_df.to_csv('/tmp/product.csv', index=False)

def merge_and_save(**kwargs):
    salesorder_df = pd.read_csv('/tmp/salesorder.csv')
    product_df = pd.read_csv('/tmp/product.csv')
    merged_df = pd.merge(salesorder_df, product_df, left_on='productid', right_on='productid')
    merged_df.to_csv('/home/vigo/merged_data/merged_data.csv', index=False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('extract_merge_save_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract_salesorder_task = PythonOperator(
        task_id='extract_salesorder',
        python_callable=extract_salesorder,
        provide_context=True
    )

    extract_product_task = PythonOperator(
        task_id='extract_product',
        python_callable=extract_product,
        provide_context=True
    )

    merge_and_save_task = PythonOperator(
        task_id='merge_and_save',
        python_callable=merge_and_save,
        provide_context=True
    )

    extract_salesorder_task >> extract_product_task >> merge_and_save_task
