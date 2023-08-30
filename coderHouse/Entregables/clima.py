import requests

url= "https://pokeapi.co/api/v2/pokemon-species?limit=20&offset=0"


r= requests.get(url, timeout=10,)


r= r.json()

print(r)