from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch, helpers
import time

# ---------- FUNGSI: FETCH DARI POSTGRES ----------
def fetch_from_postgres():
    '''
    Fungsi ini bertujuan untuk mengambil data dari PostgreSQL dan menyimpannya ke dalam file CSV 
    sebagai data mentah (raw data).

    Alur:
     - Melakukan koneksi ke database PostgreSQL menggunakan parameter koneksi yang telah ditentukan.
     - Menjalankan query untuk mengambil seluruh data dari tabel 'table_m3'.
     - Menyimpan hasilnya ke file CSV bernama 'temp_raw_data.csv'.
     - Mencoba koneksi maksimal 5 kali jika gagal.

    Return:
     None

    Contoh penggunaan:
     fetch_from_postgres()
    '''

    for i in range(5):
        try:
            conn = db.connect(
                dbname='airflow',
                user='airflow',
                password='airflow',
                host='postgres', 
                port='5432'
            )
            df = pd.read_sql("SELECT * FROM table_m3", conn)
            df.to_csv('/opt/airflow/logs/temp_raw_data.csv', index=False)
            print("Data berhasil diambil dari PostgreSQL")
            return
        except Exception as e:
            print(f"Gagal konek ke DB (percobaan {i+1}): {e}")
            time.sleep(5)

# ---------- FUNGSI: DATA CLEANING ----------
def clean_data():
    '''
    Fungsi ini digunakan untuk melakukan proses pembersihan data (data cleaning) dari file CSV 
    'temp_raw_data.csv' dan menyimpannya ke file baru setelah dibersihkan.

    Langkah-langkah pembersihan:
     - Menghapus data duplikat.
     - Mengubah seluruh nama kolom menjadi huruf kecil (lowercase).
     - Menghapus baris yang memiliki missing value.
     - Mengonversi kolom 'order_date' dan 'ship_date' menjadi format datetime (DD/MM/YYYY).
     - Menyimpan hasil data bersih ke dalam file 'P2M3_data_clean.csv'.

    Return:
     str - informasi bahwa proses data cleaning selesai ("data_cleaning_done")

    Contoh penggunaan:
     clean_data()
    '''

    df = pd.read_csv('/opt/airflow/logs/temp_raw_data.csv')

    # Hapus duplikat
    df = df.drop_duplicates()

    # Nama kolom jadi lowercase
    df.columns = df.columns.str.lower()

    # Isi missing value postal_code untuk Vermont & Burlington
    df.dropna(inplace=True)

    # Konversi order_date dan ship_date ke datetime format DD/MM/YYYY
    df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True, errors='coerce')
    df['ship_date'] = pd.to_datetime(df['ship_date'], dayfirst=True, errors='coerce')

    # Simpan data clean ke CSV
    df.to_csv('/opt/airflow/logs/data_clean.csv', index=False)

    return "data_cleaning_done"


def post_to_elasticsearch():
    '''
    Fungsi ini bertujuan untuk mengirim (upload) data yang telah dibersihkan ke Elasticsearch 
    agar dapat dianalisis dan divisualisasikan menggunakan Kibana.

    Langkah-langkah:
     - Membaca file CSV hasil data cleaning ('data_clean.csv').
     - Menginisialisasi koneksi ke Elasticsearch (host: 'elasticsearch', port: 9200).
     - Mengubah setiap baris data menjadi format dictionary dan membuang nilai yang kosong (NaN).
     - Menggunakan helpers.bulk() untuk melakukan upload data secara batch ke index 'p2m3'.

    Return:
     str - informasi jumlah dokumen yang berhasil dikirim ke Elasticsearch

    Contoh penggunaan:
     post_to_elasticsearch()
    '''
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])
    df = pd.read_csv('/opt/airflow/logs/data_clean.csv')

    actions = [
        {
            "_index": "proj3",
            "_source": row.dropna().to_dict()
        }
        for _, row in df.iterrows()
    ]
    helpers.bulk(es, actions)
    return f"Posted {len(df)} documents"
    


# ---------- DEFAULT ARGS & DAG ----------
default_args = {
    'owner': 'pat',
    'start_date': datetime(2024, 11, 2, 9, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'P2M3',
    default_args=default_args,
    schedule_interval='10 9 * * 6',
    catchup=False
) as dag:

    fetch_postgres_task = PythonOperator(
        task_id='fetch_from_postgres',
        python_callable=fetch_from_postgres
    )

    data_cleaning_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_data
    )

    post_elasticsearch_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetch_postgres_task >> data_cleaning_task >> post_elasticsearch_task
