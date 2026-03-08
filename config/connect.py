import pymongo
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from config.config import MONGO_URI

def connect():
    try:
        myclient = pymongo.MongoClient(MONGO_URI)
        print("Connect successfull!")
    except ConnectionFailure:
        print("Không thể kết nối MongoDB")

    except ServerSelectionTimeoutError:
        print("Server MongoDB không phản hồi")

    except Exception as e:
        print("Lỗi khác:", e)

# db = myclient["countly"]
# collection = db["summary"]