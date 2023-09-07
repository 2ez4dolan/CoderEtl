import requests
import os 
import connec


ciudades=["Tandil","Moron","Merlo","Ituzaingo","Gonzalez Catan","Bariloche","Necochea","Belgrano","La plata","Mar del Plata"]
api_key = os.environ.get("APY_KEY")
lenguaje= "es"

for ciudad in ciudades:
    base_url= "https://api.openweathermap.org/data/2.5/weather?"
    url = base_url+"appid="+api_key+"&q="+ciudad+"&lang="+lenguaje

    r=requests.get(url,timeout=10)

    r=r.json()
    id = r['id']
    nombre = r['name']
    pais = r['sys']['country']
    descripcion= r['weather'][0]['description']
    temp= r['main']['temp']
    feels_like = r['main']['feels_like']
    temp_max = r['main']['temp_max']
    temp_min = r['main']['temp_min']
    humedad = r['main']['humidity']
    


    print(f"id: {id} nombre: {nombre} pais: {pais} descripcion: {descripcion} temp actual: {temp} temp max: {temp_max} temp min: {temp_min} se sienten unos: {feels_like} humedad: {humedad}\n")

connec.crear_tabla()


