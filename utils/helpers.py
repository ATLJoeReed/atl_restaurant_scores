#!/usr/bin/python3.9
# -*- coding: utf-8 -*-
import datetime
import logging
import sys

import psycopg2
import psycopg2.extras
import requests

from utils import constants_sql, settings
from utils.packet_handler import PacketHandler


def build_fulton_extract_url(extraction_type, logger):
    start_date, end_date = get_date_range()
    where_clause = f"date_trunc_ymd(date) between '{start_date}' and '{end_date}'" # noqa
    if extraction_type == 'inspections':
        base_url = settings.SOCRATA_BASE_INSPECTIONS_URL
    elif extraction_type == 'violations':
        base_url = settings.SOCRATA_BASE_VIOLATIONS_URL
    else:
        logger.error(f'Invalid extraction_type: {extraction_type}')
        return None
    url = f"{base_url}$where={where_clause}"
    logger.info(f"Extract where clause: {where_clause}")
    return url


def build_geocode_url(data):
    address = data['address']
    city = data['city']
    state = data['state']
    zipcode = data['zipcode']
    base_url = settings.GEOCODE_URL
    key = settings.GEOCODE_API_KEY
    address_component = address.strip().replace(' ', '+')
    address_component = address_component + '+' + city.strip().replace(' ', '+') # noqa
    address_component = address_component + '+' + state.strip().replace(' ', '+') # noqa
    address_component = address_component + '+' + zipcode.strip()
    return f'{base_url}address={address_component}&key={key}'


def check_results(data, fields):
    fields = fields.replace('[', '').replace(']', '').replace('"','').split(',') # noqa
    for row in data:
        for field in fields:
            if field not in row.keys():
                row[field] = None
    return data


def exit_out(cur, dict_cur, conn, logger):
    logger.error('Exiting program')
    cur.close()
    dict_cur.close()
    conn.close()
    sys.exit(1)


def extract_geocodes(data, logger):
    url = build_geocode_url(data)
    try:
        results = requests.get(url)
    except Exception as e:
        logger.error(f"Extracting GeoCodes: {e}")
        return None
    status_code = results.status_code
    if status_code == 200:
        geocode = results.json()
        if geocode['status'] == 'OK':
            location = geocode['results'][-1]['geometry'].get('location') # noqa
            return {**data, **location}
        else:
            logger.info('No results found from Google GeoCode API')
            return None
    else:
        logger.error(f"Making request - status code: {status_code}")
        return None


def extract_violations(url, logger):
    headers = {"X-App-Token": settings.SOCRATA_API_TOKEN}
    results = requests.get(url, headers=headers)
    status_code = results.status_code
    if status_code == 200:
        return check_results(results.json(), results.headers['X-SODA2-Fields'])
    else:
        logger.error(f"Making request - status code: {status_code}")
        return None


def get_database_connection():
    return psycopg2.connect(**settings.DB_CONN)


def get_date_range():
    today = datetime.datetime.today()
    start_date = today - datetime.timedelta(21)
    end_date = today + datetime.timedelta(1)
    return (
        datetime.datetime.strftime(start_date, '%Y-%m-%d'),
        datetime.datetime.strftime(end_date, '%Y-%m-%d'),
        )


def get_dictionary_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def get_records2geocode(dict_cur, logger):
    dict_cur.execute(constants_sql.SELECT_GEOCODING_SQL)
    return dict_cur.fetchall()


def is_request_valid(app, conn, request):
    try:
        request_packet = request.get_json(force=True)
    except Exception as e:
        app.logger.debug(f'Error getting request packet: {e}')
        return None
    token_type = request_packet.pop('token_type', None)
    token = request_packet.pop('token', None)
    if not token_type or not token:
        return None
    packet_handler = PacketHandler(request_packet, 'get_scores')
    if not packet_handler.is_valid():
        return None
    valid_token = validate_token(token, token_type, conn)
    app.logger.debug(valid_token)
    if valid_token:
        return request_packet
    else:
        return None


def load_fulton_inspections(conn, cur, data, logger):
    try:
        logger.info('truncate table raw.fulton_inspections')
        cur.execute('truncate table raw.fulton_inspections;')
        conn.commit()
        logger.info('Inserting Fulton inspections into raw table')
        cur.executemany(constants_sql.INSERT_FULTON_INSPECTIONS_SQL, data)
        conn.commit()
        logger.info('Merging Fulton inspections')
        cur.execute(constants_sql.MERGE_FULTON_INSPECTIONS_SQL)
        conn.commit()
        return "success"
    except Exception as e:
        logger.error(f"Loading inspections into database: {e}")
        return "failure"


def load_fulton_violations(conn, cur, data, logger):
    try:
        logger.info('truncate table raw.fulton_violations')
        cur.execute('truncate table raw.fulton_violations;')
        conn.commit()
        logger.info('Inserting Fulton violations into raw table')
        cur.executemany(constants_sql.INSERT_FULTON_VIOLATIONS_SQL, data)
        conn.commit()
        logger.info('Merging Fulton violations')
        cur.execute(constants_sql.MERGE_FULTON_VIOLATIONS_SQL)
        conn.commit()
        return "success"
    except Exception as e:
        logger.error(f"Loading violations into database: {e}")
        return "failure"


def setup_logger_stdout(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def validate_token(token, token_type, conn):
    dict_cur = get_dictionary_cursor(conn)
    sql_dict = {'token': token, 'token_type': token_type}
    dict_cur.execute(constants_sql.CHECK_TOKEN, sql_dict)
    valid_token = dict_cur.fetchone().get('exists')
    dict_cur.close()
    return valid_token
