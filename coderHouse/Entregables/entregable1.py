import requests
import apikey


#base_url= "https://api.openweathermap.org/data/2.5/weather"

base_url= "http://dataservice.accuweather.com/locations/v1/cities/search?"
api_key = apikey.APY_KEYW

ciudad = "buenos aires"
url = base_url+"apikey="+api_key+"&q="+ciudad


r=requests.get(url,timeout=10)

r=r.json()[0]

city_key=r["Key"]

print(city_key)

