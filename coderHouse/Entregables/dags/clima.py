import requests
import os 
import connec
import pandas as pd 
from datetime import datetime, timedelta
from airflow.models import Variable




ciudades=["Tandil","Moron","Merlo","Ituzaingo","Gonzalez Catan","Bariloche","Necochea","Belgrano","La plata","Mar del Plata"]

def directorios():
    separador = os.path.sep
    dir_actual = os.path.dirname(os.path.abspath(__file__))
    dir_ant = separador.join(dir_actual.split(separador)[:-1])
    dir= os.path.join(dir_ant,'output')
    return dir
    


 

#EXTRAIGO INFORMACION DE LA API Y LA GUARDO EN UN ARCHIVO CSV
def extract_api():
    
    #api_key = os.environ.get("APY_KEY")
    api_key = Variable.get("SECRET_API_KEY")
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
    file_output = os.path.join(directorios(), f'extraccion_{fecha_actual.strftime("%Y%m%d")}.csv')
    df.to_csv(file_output, index=False)
    print("extraccion realizada")


#CONVIERTO LOS GRADOS A CELCIUS
def grados_celcius(kelvin):

    return kelvin - 273.15

#BORRO DUPLICADOS, LEO EL ARCHIVO DE EXTRACCION Y LE APLICO LA FUNCION DE GRADOS , ADEMAS DE GENERAR UNA COLUMNA DE SENSACION TERMICA Y GENERAR UN NUEVO ARCHIVO CON LA INFO MODIFICADA PARA CARGARLA
def transform_data():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    file_extraccion = os.path.join(directorios(), f'extraccion_{fecha_actual}.csv')
    df = pd.read_csv(file_extraccion)
    df= df.drop_duplicates()
    connec.crear_tabla()
    df.loc[:,["temp","temp_max","temp_min"]] = df.loc[:,["temp","temp_max","temp_min"]].applymap(grados_celcius).round(0)
    df["sensacion_termica"]=df["feels_like"].apply(grados_celcius).round(0)
    print(df)
    file_transform= os.path.join(directorios(), f'cargar_{fecha_actual}.csv')
    df.to_csv(file_transform, index=False)


#LEO ESTE ULTIMO ARCHIVO TRANSFORMADO Y LO CARGO EN LA BASE
def insert_data():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    file_transform= os.path.join(directorios(), f'cargar_{fecha_actual}.csv')
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