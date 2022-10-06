from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from config import clickhouse_connection
from config import data_dir 
# Интерфейс Pandas для HTTP API Clickhouse
from pandahouse import *


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Подготовка данных для анализа 
def prepare_df(filepath):
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath, sep=',')
        df['date'] = df['starttime'].apply(
            lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))  # создаем колонку date и превращаем её в объект datetime
        df['date'] = df['date'].dt.date  # форматируем колонку в тип date
              
    return df


# Количество поездок в день
def number_of_trips():
    for file in data_dir:
        try:
            df = prepare_df(file)
            # считаем строки и записываем в таблицу clickhouse
            df = df.value_counts('date').to_frame(name='count')
            to_clickhouse(df, table='trips_count',
                        connection=clickhouse_connection)
        except:
            continue


#  Средняя продолжительность поездок в день
def avg_trip_duration():
    for file in data_dir:
        try:
            df = prepare_df(file)

            df_count_sum_trips = df.groupby('date')['tripduration'].agg(
                count_trips='count', sum_trip_seconds='sum')  # группируем по дате, аггрегируем количество и сумму поездок
            df_count_sum_trips['avg_duration'] = df_count_sum_trips['sum_trip_seconds'] / \
                df_count_sum_trips['count_trips']  # создаем колонку средней продолжительности поездки.
            df_date_avg = df_count_sum_trips[['avg_duration']].round(
                decimals=2)  # округляем и записываем таблицу clickhouse
            to_clickhouse(df_date_avg, table='average_trips_duration',
                            connection=clickhouse_connection)
        except:
            continue


# Распределение поездок пользователей, разбитых по категории «gender»
def gender_count():
    for file in data_dir:
        try:
            df = prepare_df(file)
            df_gender_count = df.groupby(['date', 'gender'])[
                'tripduration'].agg(count='count')  # группируем по стобцам date, gender и аггрегируем количество поездок
            to_clickhouse(df_gender_count, table='gender_count',
                            connection=clickhouse_connection)
        except:
            continue 


#  Забираем данные из таблиц clickhouse и формируем отчеты 
def statistics_sending():
    trips_count_df = read_clickhouse(
        'SELECT * FROM bicycle_trips.trips_count', connection=clickhouse_connection)
    average_trip_duration_df = read_clickhouse(
        'SELECT * FROM bicycle_trips.average_trips_duration', connection=clickhouse_connection)
    gender_count_df = read_clickhouse(
        'SELECT * FROM bicycle_trips.gender_count', connection=clickhouse_connection)

    trips_count_df.to_csv('/opt/airflow/statistics/{{ ds }}/trips_count.csv')
    average_trip_duration_df.to_csv(
        '/opt/airflow/statistics/{{ ds }}/average_trip_duration.csv')
    gender_count_df.to_csv(
        '/opt/airflow/statistics/{{ ds }}/gender_count.csv')


# Чистим директорию с данными
def cleaning_directory():
    directory = os.listdir(
        '/opt/airflow/data_folder')
    for file in directory:
        os.remove(file)


with DAG("diploma_project_airflow", default_args=default_args, schedule_interval='@daily', max_active_runs=2) as dag:

    checking_files = FileSensor(
        task_id='checking_files',
        filepath='data_folder',
        fs_conn_id='my_file_system',
        poke_interval=10
    )
    
    # Движок ReplacingMergeTree чистит дублирующиеся записи 
    creating_clickhouse_tables = ClickHouseOperator(
        task_id = 'creating_clickhouse_tables',
        sql = (
            ''' CREATE TABLE IF NOT EXISTS bicycle_trips.trips_count (date Date, count UInt64) ENGINE=ReplacingMergeTree(date, (date), 8192) ''',
            ''' CREATE TABLE IF NOT EXISTS bicycle_trips.average_trips_duration (date Date, avg_duration Float64) ENGINE=ReplacingMergeTree(date, (date), 8192) ''',
            ''' CREATE TABLE IF NOT EXISTS bicycle_trips.gender_count (date Date, gender UInt8, count UInt64) ENGINE=ReplacingMergeTree(date, (date), 8192) ''',
            ),
        clickhouse_conn_id = 'bicycle_trips'
    )

    trips_count = PythonOperator(
        task_id='trips_count',
        python_callable=number_of_trips
    )

    avg_duration = PythonOperator(
        task_id='avg_duration',
        python_callable=avg_trip_duration
    )

    count_gender = PythonOperator(
        task_id='count_gender',
        python_callable=gender_count
    )

    creating_statistics_dirs = BashOperator(
        task_id='creating_statistics_dir',
        # создаем папку для хранения статистики с использованием шаблонов jinja ( ds = execution_date в формате “yyyy-mm-dd” )
        bash_command='mkdir /opt/airflow/statistics/{{ ds }}'
    )

    sending_statistics = PythonOperator(
        task_id = 'sending_statistics',
        python_callable = statistics_sending
    )

    removing_files_from_folder = PythonOperator(
        task_id='removing_files_from_folder',
        python_callable = cleaning_directory
    )


checking_files >> creating_clickhouse_tables >> [trips_count, avg_duration, count_gender] >> creating_statistics_dirs >> \
sending_statistics >> removing_files_from_folder

