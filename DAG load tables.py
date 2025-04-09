from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Константы
DB_CONN = "std11_12"
DB_SCHEMA = 'std11_12'

# Определение таблиц и файлов для загрузки
FULL_LOAD_TABLES = ['stores', 'coupons', 'promos', 'promo_types']
FULL_LOAD_FILES = {
    'stores': 'stores',
    'coupons': 'coupons',
    'promos': 'promos',
    'promo_types': 'promo_types'
}

# Запрос для загрузки данных через gpfdist
MD_TABLE_LOAD_QUERY = """
    SELECT std11_12.f_load_gpfdist('{tab_name}', '{file_name}');
"""

# Параметры DAG
default_args = {
    'depends_on_past': False,
    'owner': 'std11_12',
    'start_date': datetime(2025, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создание DAG
with DAG(
    dag_id="std11_12",
    description="DAG для полной загрузки таблиц из csv",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    # Задачи
    task_start = EmptyOperator(task_id="start")
    

    # Группа задач для полной загрузки таблиц
    with TaskGroup("full_insert") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
             PostgresOperator(
                task_id=f"load_table_{table}",
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_LOAD_QUERY.format(
                    tab_name=f"{DB_SCHEMA}.{table}",
                    file_name=FULL_LOAD_FILES[table]
                )
            )

    task_end = EmptyOperator(task_id="end")

    # Последовательность выполнения
    task_start >> task_full_insert_tables >> task_end
