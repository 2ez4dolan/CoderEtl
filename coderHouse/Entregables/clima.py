import requests
import os 
import connec
import pandas as pd 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



def extract_api():
    ciudades=["Tandil","Moron","Merlo","Ituzaingo","Gonzalez Catan","Bariloche","Necochea","Belgrano","La plata","Mar del Plata"]
    api_key = os.environ.get("APY_KEY")
    lenguaje= "es"
    response=[]
    fecha_actual= datetime.today()
    for ciudad in ciudades:
        base_url= "https://api.openweathermap.org/data/2.5/weather?"
        url = base_url+"appid="+api_key+"&q="+ciudad+"&lang="+lenguaje

        r=requests.get(url,timeout=10)
        r=r.json()
  
        response.append({"id":r['id'] ,"nombre":r['name'],"pais":r['sys']['country'],"descripcion":r['weather'][0]['description'],"temp": r['main']['temp'],"feels_like" : r['main']['feels_like'],"temp_max": r['main']['temp_max'],"temp_min" : r['main']['temp_min'], "humedad" : r['main']['humidity'],"fecha_solicitud":fecha_actual})
        df= pd.DataFrame(response)
    print(df)
    return  df 

def grados_celcius(kelvin):

    return kelvin - 273.15

def transform_data(df):
    df= df.drop_duplicates()
    connec.crear_tabla()
    df.loc[:,["temp","temp_max","temp_min"]] = df.loc[:,["temp","temp_max","temp_min"]].applymap(grados_celcius).round(0)
    df["sensacion_termica"]=df["feels_like"].apply(grados_celcius).round(0)
    print(df)
    return df


def insert_data(df):
    for index,row in df.iterrows():
        connec.insertar(
            id = row['id'],
            nombre = row['nombre'],
            pais = row['pais'],
            descripcion= row['descripcion'],
            temp= row['temp'],
            sensacion = row['sensacion_termica'],
            temp_max = row['temp_max'],
            temp_min = row['temp_min'],
            humedad = row['humedad'],
            fecha_actual=row['fecha_solicitud']
            )   
    connec.cerrar()


default_args={
    'owner': 'DavidBU',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}


with DAG(
    default_args=default_args,
    dag_id='Clima_Api',
    description= 'Dag de etl de api del clima',
    start_date=datetime(2022,8,1,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='extract_api',
        python_callable= extract_api,
    )
    task2= PythonOperator(
        task_id='transform_data',
        python_callable= transform_data,
    )
    task3= PythonOperator(
        task_id='insert_data',
        python_callable= insert_data,
    )

    task1 >> task2 >> task3 