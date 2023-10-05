import requests
import os 
import connec
import pandas as pd 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator




def extract_api():
    carpeta_actual = os.path.dirname(__file__)
    carpeta_output = os.path.join(carpeta_actual,'..','output')
    ciudades=["Tandil","Moron","Merlo","Ituzaingo","Gonzalez Catan","Bariloche","Necochea","Belgrano","La plata","Mar del Plata"]
    #api_key = os.environ.get("APY_KEY")
    api_key = "da172fafe18d867d54ab55818873b798"
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
    file_output = os.path.join(carpeta_output, f'extraccion_{fecha_actual.strftime("%Y%m%d")}.csv')
    df.to_csv(file_output, index=False)



def grados_celcius(kelvin):

    return kelvin - 273.15

def transform_data():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    carpeta_output = os.path.join(os.path.dirname(__file__),'..','output')
    file_output = os.path.join(carpeta_output, f'extraccion_{fecha_actual}.csv')
    df = pd.read_csv(file_output)
    df= df.drop_duplicates()
    connec.crear_tabla()
    df.loc[:,["temp","temp_max","temp_min"]] = df.loc[:,["temp","temp_max","temp_min"]].applymap(grados_celcius).round(0)
    df["sensacion_termica"]=df["feels_like"].apply(grados_celcius).round(0)
    print(df)
    file_transform= os.path.join(carpeta_output, f'cargar_{fecha_actual}.csv')
    df.to_csv(file_transform, index=False)


def insert_data():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    carpeta_output = os.path.join(os.path.dirname(__file__),'..','output')
    file_transform= os.path.join(carpeta_output, f'cargar_{fecha_actual}.csv')
    df = pd.read_csv(file_transform)

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
    'owner': 'Airflow',
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