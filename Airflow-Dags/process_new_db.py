from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

# Fungsi untuk membaca CSV dan memuat data ke MongoDB
def load_csv_to_mongodb():
    # Baca file CSV
    df = pd.read_csv('/home/vigo/merged_data/output_final_olap/final_olap.csv')
    
    # Buat koneksi ke MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bikessales_olap']
    collection = db['salesorder']
    
    # Konversi DataFrame ke dict dan muat ke MongoDB
    data_dict = df.to_dict("records")
    collection.insert_many(data_dict)
    
    # Tutup koneksi
    client.close()

# Definisikan default_args untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Inisialisasi DAG
dag = DAG(
    'csv_to_mongodb',
    default_args=default_args,
    description='A simple DAG to load CSV data to MongoDB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Definisikan task menggunakan PythonOperator
csv_to_mongodb = PythonOperator(
    task_id='load_csv_to_mongodb_task',
    python_callable=load_csv_to_mongodb,
    dag=dag,
)

# Set task dependencies
csv_to_mongodb
