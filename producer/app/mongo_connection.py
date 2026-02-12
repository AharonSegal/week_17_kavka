import os

from pymongo import MongoClient

mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
mongo_db = os.getenv("MONGO_DB", "suspicious")
mongo_collection = os.getenv("MONGO_COLLECTION", "records")

client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]