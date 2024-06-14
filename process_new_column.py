from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import pandas as pd

def reorder_csv_columns(**kwargs):
    input_path = '/home/vigo/merged_data/merged_data.csv'
    output_path = '/home/vigo/merged_data/new_merged_data.csv'
    
    # Membaca CSV
    df = pd.read_csv(input_path)
    
    # Menyusun ulang kolom
    columns_order = [
        "salesorderid", "salesorderitem", "productid", "quantity", "weightmeasure",
        "weightunit", "currency", "price", "grossamount", "taxamount", "netamount",
        "language", "medium_descr", "short_descr", "deliverydate", "prodcategoryid",
        "salesorg"
    ]
    df = df.reindex(columns=columns_order)
    
    # Menyimpan CSV yang sudah diubah
    df.to_csv(output_path, index=False)

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Definisi DAG
with DAG('reorder_csv_columns_dag',
    default_args=default_args,
    schedule_interval= timedelta(days=1),
    start_date=datetime(2024,1,1),
    catchup= False
) as dag:

    reorder_task = PythonOperator(
        task_id='reorder_csv_columns',
        python_callable=reorder_csv_columns,
        provide_context=True,
    )

    reorder_task