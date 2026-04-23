import requests
import json

url = "https://api.openweathermap.org/data/2.5/weather?q=Salvador&appid=SUA_CHAVE"

response = requests.get(url)
data = response.json()

with open('weather_data.json', 'w') as f:
    json.dump(data, f, indent=4)

print(data)