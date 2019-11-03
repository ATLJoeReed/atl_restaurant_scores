#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
import sys

from flask import Flask, request, jsonify
from flask_restful import Resource, Api

from config import constants_sql, settings
from utils import helpers


app = Flask(__name__)
api = Api(app)


try:
    conn, cur, dict_cur = helpers.get_database_connection()
except Exception:
    sys.exit("Unable to get database connection")


def is_request_valid(request):
    try:
        request_packet = request.get_json(force=True)
    except Exception as e:
        app.logger.error(f"Getting request packet: {e}")
        return None
    app.logger.debug(request.headers)
    token_type = request_packet.pop('token_type', None)
    token = request_packet.pop('token', None)
    if not token_type or not token:
        return None
    sql_dict = {'token': token, 'token_type': token_type}
    dict_cur.execute(constants_sql.CHECK_TOKEN, sql_dict)
    valid_token = dict_cur.fetchone().get('exists')
    app.logger.debug(f'Valid Token: {valid_token}')
    if valid_token:
        return request_packet
    else:
        return None


class GetRestaurantScores(Resource):
    def get(self):
        request_packet = is_request_valid(request)
        app.logger.debug(request_packet)
        if not request_packet:
            return jsonify(status='invalid_request')
        latitude = request_packet.get('latitude')
        longitude = request_packet.get('longitude')
        num_scores = request_packet.get('num_scores', 10)
        sql_dict = {
            'latitude': latitude,
            'longitude': longitude,
            'num_scores': num_scores,
        }
        dict_cur.execute(constants_sql.FETCH_SCORES, sql_dict)
        scores = dict_cur.fetchall()
        return jsonify(scores)

# TODO: Need to create a database logger to keep all request/responses...


api.add_resource(
    GetRestaurantScores,
    '/restaurant/api/v1.0/scores',
    methods=['GET']
)


if __name__ == '__main__':
    app.run(debug=settings.DEBUG)
