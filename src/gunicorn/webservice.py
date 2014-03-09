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

__all__ = ['spawn_webservice']

def spawn_webservice():
    application = flask.Flask(__name__)

    api=Api(application)

    client = pymongo.MongoClient()

    class list_databases(flask.ext.restful.Resource):
        def get(self):
            return client.database_names()

    class list_categories(flask.ext.restful.Resource):
        def get(self):
            #return client.INSEE.collection_names()
            return client.eurostat.collection_names()

    class category_eurostat(flask.ext.restful.Resource):
        def get(self):
            #item = client.eurostat.categories3.find_one({"name2" : "Database by themes"})
            item = client.eurostat.categories.find()
            return dumps(item)

    api.add_resource(list_databases, '/list_databases')
    api.add_resource(list_categories, '/list_categories')
    api.add_resource(category_eurostat, '/category_eurostat')

    @application.route('/')
    def index():
        return "test index!"

    application.wsgi_app = ProxyFix(application.wsgi_app)

    application.run(debug=True)

#gunicorn --bind=127.0.0.1:8001 webservice:App 
#curl -i http://127.0.0.1:8001/cat_eurostat
