"""Kiểm tra MongoDB bằng pymongo."""
from pymongo import MongoClient
c = MongoClient("mongodb://mongodb:27017", serverSelectionTimeoutMS=5000)
col = c.kiem_tra.giao_dich
col.delete_many({})
col.insert_many([{"region": r, "amount": a} for r, a in [("HCM", 100), ("Hanoi", 80), ("HCM", 50)]])
res = list(col.aggregate([{"$group": {"_id": "$region", "tong": {"$sum": "$amount"}}}, {"$sort": {"_id": 1}}]))
import pymongo
print("pymongo", pymongo.version, "mongod", c.server_info()["version"], res)
