#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
import datetime
import logging
import sys

import psycopg2
import psycopg2.extras
import requests

from config import constants_sql, settings


def build_extract_url(logger):
    start_date, end_date = get_date_range()
    base_url = settings.SOCRATA_BASE_URL
    url = f"{base_url}$where=date_trunc_ymd(date) between '{start_date}' and '{end_date}'" # noqa
    logger.info(f"Extract URL: {url}")
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


def extract_geocodes(data, logger):
    url = build_geocode_url(data)
    results = requests.get(url)
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


def extract_inspections(url, logger):
    headers = {"X-App-Token": settings.SOCRATA_API_TOKEN}
    results = requests.get(url, headers=headers)
    status_code = results.status_code
    if status_code == 200:
        return check_results(results.json(), results.headers['X-SODA2-Fields'])
    else:
        logger.error(f"Making request - status code: {status_code}")
        return None


def get_database_connection():
    conn = psycopg2.connect(**settings.DB_CONN)
    cur = conn.cursor()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur, dict_cur)


def get_date_range():
    today = datetime.datetime.today()
    start_date = today - datetime.timedelta(21)
    end_date = today + datetime.timedelta(1)
    return (
        datetime.datetime.strftime(start_date, '%Y-%m-%d'),
        datetime.datetime.strftime(end_date, '%Y-%m-%d'),
        )


def get_records2geocode(dict_cur, logger):
    dict_cur.execute(constants_sql.SELECT_GEOCODING_SQL)
    return dict_cur.fetchall()


def load_inspections(conn, cur, data, logger):
    try:
        cur.execute("truncate table raw.inspections;")
        conn.commit()
        cur.executemany(constants_sql.INSERT_INSPECTIONS_SQL, data)
        conn.commit()
        cur.execute(constants_sql.MERGE_INSPECTIONS_SQL)
        conn.commit()
        return "success"
    except Exception as e:
        logger.error(f"Loading inspections into database: {e}")
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