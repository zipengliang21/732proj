from flask import Flask
from pymongo import MongoClient

app = Flask(__name__)
app.config["SECRET_KEY"] ='657fe4b48c16dab9ba6804334bbcf1d9e77f4d75'

client = MongoClient('')
db=client.get_database('proj_db')





from application import routes