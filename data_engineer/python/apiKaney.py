# api kenye
import requests
url = "https://api.kanye.rest"
response = requests.post(url)
print(response.text)

