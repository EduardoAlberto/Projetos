from fastapi import FastAPI
from routes.route import router

import certifi

app = FastAPI()

app.include_router(router)

# uri = "mongodb+srv://eduardoadsantos:Admin123@cluster0.3b4yvax.mongodb.net/?appName=Cluster0"

# client = MongoClient(
#     uri,
#     server_api=ServerApi('1'),
#     tls=True,
#     tlsCAFile=certifi.where()
# )

# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print("Erro:", e)
