#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
from flask import Flask, g, jsonify, request
from flask_restful import Resource, Api

from config import constants_sql, settings
from utils import helpers


app = Flask(__name__)
api = Api(app)


@app.before_request
def before_request():
    g.conn = helpers.get_database_connection()


@app.teardown_request
def teardown_request(exception):
    g.conn.close()


class GetRestaurantScores(Resource):
    def get(self):
        request_packet = helpers.is_request_valid(app, g.conn, request)
        app.logger.debug(request_packet)
        if not request_packet:
            return jsonify(status='invalid_request')
        dict_cur = helpers.get_dictionary_cursor(g.conn)
        dict_cur.execute(constants_sql.FETCH_SCORES, request_packet)
        scores = dict_cur.fetchall()
        dict_cur.close()
        return jsonify(scores)

# TODO: Need to create a database logger to keep all request/responses...


api.add_resource(
    GetRestaurantScores,
    '/restaurant/api/v1.0/scores',
    methods=['GET']
)


if __name__ == '__main__':
    app.run(debug=settings.DEBUG)
