#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import flask
import pymongo
from flask.ext.restful import Api
from flask import Flask
from werkzeug.contrib.fixers import ProxyFix
import bson
from bson import json_util
from bson.json_util import dumps

#application instance
App = flask.Flask(__name__)

# our api instance
api=Api(App)

client = pymongo.MongoClient()

class list_dbs(flask.ext.restful.Resource):
    def get(self):
        return client.database_names()

class list_cat(flask.ext.restful.Resource):
    def get(self):
        #return client.INSEE.collection_names()
        return client.eurostat.collection_names()

class cat_eurostat(flask.ext.restful.Resource):
    def get(self):
        #item = client.eurostat.categories3.find_one({"name2" : "Database by themes"})
        item = client.eurostat.categories.find()
        return dumps(item)

api.add_resource(list_dbs, '/list_dbs')
api.add_resource(list_cat, '/list_cat')
api.add_resource(cat_eurostat, '/cat_eurostat')

@App.route('/')
def index():
    return "test index!"

App.wsgi_app = ProxyFix(App.wsgi_app)

if __name__ == '__main__':
    App.run(debug=True)

#gunicorn --bind=127.0.0.1:8001 webservice:App 
#curl -i http://127.0.0.1:8001/cat_eurostat
