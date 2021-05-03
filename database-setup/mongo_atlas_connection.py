from pymongo import MongoClient

client = MongoClient("mongodb+srv://root:root@cluster0.dswdu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('Intelligent-Email-App')

