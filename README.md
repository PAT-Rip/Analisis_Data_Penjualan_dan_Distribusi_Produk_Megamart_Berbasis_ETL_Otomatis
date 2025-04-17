# Analisis_Data_Penjualan_dan_Distribusi_Produk_Megamart_Berbasis_ETL_Otomatis
Saya membangun pipeline ETL otomatis untuk menganalisis penjualan dan distribusi produk Megamart. Data diproses dari PostgreSQL, dibersihkan dan divalidasi dengan Python &amp; Great Expectations, lalu divisualisasikan melalui dashboard interaktif di Kibana.

## Repository Outline
```
1. Folder images: Folder yang berisikan screenshot dari dashboard yang sudah dibuat di kibana 
2. DAG_graph.jpg: Gambar dari graph workflow yang sudah dilakukan di airflow
3. DAG.py: Berisikan DAG workflow airflow yang digunakan untuk mengambil data dari postgres, cleaning data, dan post ke elasticsearch
4. data_clean.csv: Tabel CSV hasil dari cleaning data yang dilakukan DAG
5. data_raw.csv: Dataset raw yang digunakan dalam project ini
6. Great_Expectations.ipynb: Berisikan hasil test yang dilakukan menggunakan Great Expectation
```

## Problem Background
Dunia retail saat ini semakin kompetitif. Maka dari itu untuk tetap unggul megamart perlu tau dengan baik bagaimana performa penjualan produk, bagaimana efisiensi pengiriman, serta kontribusi produk di setiap wilayah terhadap profit perusahaan

## Project Output
Dashboard yang berisikan insight dari hasil analisis yang sudah dilakukan yang dibuat menggunakan kibana

## Data
Dataset diambil dari kaggle yang bernama Superstores Sales, Dataset ini memiliki 18 kolom dan kurang lebih 9000 row

## Method
Dalam menyelesaikan project ini saya menggunakan airflow untuk membuat workflow dan menyimpan hasil data cleaning yang sudah dilakukan kedalam elasticsearch dan membuat dashboard menggunakan kibana

## Stacks
```
1. Python
2. SQL
3. Pandas
4. SQLalchemy
5. airflow
6. Psychopg2
7. elasticsearch
8. Great Expectation
```

## Reference
` https://mediaindonesia.com/ekonomi/743663/gerak-cepat-bisnis-ritel-beradaptasi-pada-perubahan-tren-dan-teknologi: latar belakang diangkat dari artikel ini`


