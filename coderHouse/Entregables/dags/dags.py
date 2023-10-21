from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import clima
import alerta

default_args={
    'owner': 'Airflow',

}

#DEFINO MI DAG CON MIS 3 TAREAS DE ETL
with DAG(
    default_args=default_args,
    dag_id='Clima_Api',
    description= 'Dag de etl de api del clima',
    start_date=days_ago(1),  
    schedule_interval='0 9 * * *' 
    ) as dag:
    task1= PythonOperator(
        task_id='extract_api',
        python_callable= clima.extract_api,
    )
    task2= PythonOperator(
        task_id='transform_data',
        python_callable= clima.transform_data,
    )
    task3= PythonOperator(
        task_id='insert_data',
        python_callable= clima.insert_data,
    )
    task4= PythonOperator(
        task_id='alerta_min',
        python_callable= alerta.alertaMinima,
    )
     task5= PythonOperator(
        task_id='alerta_max',
        python_callable= alerta.alertaMaxima,
    )


    task1 >> task2 >> task3 >> task4 >> task5