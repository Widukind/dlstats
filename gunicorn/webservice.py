import flask
import pymongo

app = flask.Flask(__name__)
api = flask.ext.restful.Api(app)
client = pymongo.MongoClient()

class list_dbs(flask.ext.restful.Resource):
    def get(self):
        return client.database_names()

api.add_resource(list_dbs, '/list_dbs')

if __name__ == '__main__':
    app.run(debug=True)
