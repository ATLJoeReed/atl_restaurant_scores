#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
import os
import sys

from config import constants_sql
import utils


def process_inspections(logger):
    try:
        conn, cur, dict_cur = utils.get_database_connection()
    except Exception as e:
        logger.error(f"Getting database connection: {e}")
        sys.exit("Unable to get database connection")

    url = utils.build_extract_url(logger)
    logger.info("Starting the extract")
    results = utils.extract_inspections(url, logger)
    number_results = len(results)
    logger.info(f"Extracted {number_results} records")
    load_status = utils.load_inspections(conn, cur, results, logger)
    logger.info(f"Load status: {load_status}")

    geocode_updates = []
    results = utils.get_records2geocode(dict_cur, logger)
    number_results = len(results)
    logger.info(f"{number_results} records to GeoCode")
    for row in results:
        geocode_results = utils.extract_geocodes(row, logger)
        if geocode_results:
            geocode_updates.append(geocode_results)
    logger.info('GeoCoding complete')

    cur.executemany(constants_sql.UPDATE_GEOCODING_SQL, geocode_updates)
    conn.commit()

    conn.close()
    cur.close()
    dict_cur.close()


if __name__ == '__main__':
    # move to working directory...
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    logger = utils.setup_logger_stdout('process_inspections')

    process_inspections(logger)