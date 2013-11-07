#!/usr/bin/python3
import flask
import pymongo

from flask.ext.restful import Api
from werkzeug.contrib.fixers import ProxyFix

#application instance
app = flask.Flask(__name__)

# our api instance
api=Api(app)

#client=connection = MongoClient()
client = pymongo.MongoClient()

#a ressource class that will be mapped to a URL
# list_dbs derived from Resource
class list_dbs(flask.ext.restful.Resource):
    def get(self):
        return client.database_names()

class list_cat(flask.ext.restful.Resource):
    def get(self):
        return client.INSEE.collection_names()

# map the list_dbs ressource to '/list_dbs' endpoint
api.add_resource(list_dbs, '/list_dbs')
api.add_resource(list_cat, '/list_cat')

@app.route('/')
def index():
    return "test index!"

app.wsgi_app = ProxyFix(app.wsgi_app)

if __name__ == '__main__':
    app.run(debug=True)
