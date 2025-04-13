from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
  

# Данные для подключения константы
  
DB_CONN = "std11_12"

DB_SCHEMA = 'std11_12'


# Загрузка данных через Delta partition  
  
CURRENT_YEAR = datetime.now().year

CURRENT_MONTH = datetime.now().month

FIRST_DAY  = datetime(CURRENT_YEAR,CURRENT_MONTH,1)
 
DB_PROC_DELTA_LOAD = 'f_load_partition'
 
DELTA_TABLES = ['std11_12.bills_head', 'std11_12.bills_item', 'std11_12.traffic']
 
PARTITION_KEYS = {'std11_12.bills_head':'calday', 'std11_12.bills_item':'calday', 'std11_12.traffic':'date'}

PXF_TABLES = {'std11_12.bills_head':'gp.bills_head', 'std11_12.bills_item':'gp.bills_item', 'std11_12.traffic':'gp.traffic'}

DELTA_PARTITION_QUERY = f"select {DB_SCHEMA}.{DB_PROC_DELTA_LOAD}(%(table)s,%(partition_key)s,%(pxf_table)s,%(user_id)s,%(pass)s,%(start_date)s,%(end_date)s);"

 
# Default args

default_args = {
    'depends_on_past': False,
    'owner': 'user',
    'start_date': datetime(2025,4,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    "f_load_partition",
    max_active_runs=3,
    schedule_interval= '00 23 * * *',
    default_args=default_args,
    catchup=False,
    tags=["std11_12"]
) as dag:
    
    task_start = EmptyOperator(task_id="start")
                             
    with TaskGroup("delta_load") as delta_load:
        for table in DELTA_TABLES:
            task = PostgresOperator(task_id=f"data_delta_load_{table}",
                                postgres_conn_id = DB_CONN,
                                sql=DELTA_PARTITION_QUERY,
                                 parameters={
                                'table': table,
                                'partition_key': PARTITION_KEYS[table],
                                'pxf_table': PXF_TABLES[table], 
                                 'user_id': 'intern',
                                 'pass': 'intern', 
                                 'start_date':datetime.strptime('2020-01-01'),
                                 'end_date':datetime.strptime('2023-01-01')
                                    }
                                   )
 
    task_delta_report = EmptyOperator(task_id="delta_exchanged")
    
    task_end = EmptyOperator(task_id="end")
    
    
    task_start>>delta_load>>task_delta_report>>task_end
