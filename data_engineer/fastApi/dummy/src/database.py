from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi

MONGO_URI = "mongodb+srv://eduardoadsantos:Admin123@cluster0.3b4yvax.mongodb.net/?appName=Cluster0"

client = MongoClient(
    MONGO_URI,
    server_api=ServerApi('1'),
    tls=True,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=5000
)

db = client["todo_db"]
collection_name = db["todo_collection"]
