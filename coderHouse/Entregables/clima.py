import requests
import os 
import connec
import pandas as pd 
import datetime

ciudades=["Tandil","Moron","Merlo","Ituzaingo","Gonzalez Catan","Bariloche","Necochea","Belgrano","La plata","Mar del Plata"]
api_key = os.environ.get("APY_KEY")
lenguaje= "es"
response=[]
fecha_actual= datetime.date.today()
for ciudad in ciudades:
    base_url= "https://api.openweathermap.org/data/2.5/weather?"
    url = base_url+"appid="+api_key+"&q="+ciudad+"&lang="+lenguaje

    r=requests.get(url,timeout=10)
    r=r.json()
  
    response.append({"id":r['id'] ,"nombre":r['name'],"pais":r['sys']['country'],"descripcion":r['weather'][0]['description'],"temp": r['main']['temp'],"feels_like" : r['main']['feels_like'],"temp_max": r['main']['temp_max'],"temp_min" : r['main']['temp_min'], "humedad" : r['main']['humidity'],"fecha_solicitud":fecha_actual})
   

df = pd.DataFrame(response)

df= df.drop_duplicates()
connec.crear_tabla()

def grados_celcius(kelvin):

    return kelvin - 273.15




df.loc[:,["temp","temp_max","temp_min"]] = df.loc[:,["temp","temp_max","temp_min"]].applymap(grados_celcius).round(0)

df["sensacion_termica"] = df["feels_like"].apply(grados_celcius).round(0)



print (df)
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