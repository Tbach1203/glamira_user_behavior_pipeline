import pymongo
from config.config import MONGO_URI

def connect():
    try:
        myclient = pymongo.MongoClient(MONGO_URI)
        myclient.admin.command("ping")
        print("MongoDB connected!")
        return myclient
    
    except Exception as e:
        print("MongoDB connection failed:", e)
        return None