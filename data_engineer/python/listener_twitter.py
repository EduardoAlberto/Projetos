import json
import requests
import socket

HOST = 'localhost'
PORT = 9009
s = socket.socket()
s.bind((HOST, PORT))
print(f"Aguardando conexão na porta: {PORT}")

s.listen(5)
connection, address = s.accept()
print(f"Recebendo solicitação de {address}")

# BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAGxoUwEAAAAAVVduJAZPY3NUWDIG8rm1K5LbCmo%3DKnHVHkK5G3MRHBjp6S7LW3b3gWLKkCPqLRPa5MKylSboAFijBw'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAFWDiwEAAAAAJHPPQybizSpPQYyF76P2JU5XR4k%3DEGC72Y5msjiJD82oWmBt6MraEoL0HGM9aFk7xhWcv6mOxVnUNn'
# POST PARA O RULES PARA DEFINIR O QUE VAI BUSCAR
keyword = "futebol"
url_rules = "https://api.twitter.com/2/tweets/search/stream/rules"
header = headers={'Authorization': f"Bearer {BEARER_TOKEN}"}
response = requests.post(url_rules,headers=header, json ={"add": [{"value": keyword}]})

#GET PARA PEGAR OS DADOS
url = "https://api.twitter.com/2/tweets/search/stream"
response = requests.get(url, headers=header, stream=True)

if response.status_code == 200:
    for item in response.iter_lines():
        print(json.loads(item)['data']["text"])
        print("="*50)
        connection.send(json.loads(item)['data']["text"].encode('utf-8', 'ignore'))

connection.close()