import requests

url = "https://danepubliczne.imgw.pl/api/data/hydro"

response = requests.get(url)

print("Status:", response.status_code)

data = response.json()

print("Liczba rekordów:", len(data))

print("Pierwszy rekord:")
print(data[0])