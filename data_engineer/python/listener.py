from socket import socket
import time

HOST = "localhost"
PORT = 3000

s = socket()
s.bind((HOST, PORT))
print(f"Aguardando conexào na porta :{PORT}")

s.listen(5)
conn,address = s.accept()

print(f"Recebendo solicitação do {address}")

messages = [
    "Mensagem A" \
    "Mensagem B" \
    "Mensagem C" \
    "Mensagem D" \
    "Mensagem E" \
    "Mensagem F" \
    "Mensagem G" \
    "Mensagem H" \
]

for message in messages:
    message = bytes(message, "utf-8")
    conn.send(message)
    time.sleep(4)

conn.close()

