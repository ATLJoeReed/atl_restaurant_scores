#!/usr/bin/python3.9
# -*- coding: utf-8 -*-
import os
import sys

from utils import constants_sql, helpers


def process_inspections(logger):
    try:
        conn = helpers.get_database_connection()
    except Exception as e:
        logger.error(f"Getting database connection: {e}")
        sys.exit("Unable to get database connection")

    cur = conn.cursor()
    dict_cur = helpers.get_dictionary_cursor(conn)

    url = helpers.build_extract_violations_url(logger)
    logger.info("Starting the extract")
    results = helpers.extract_violations(url, logger)
    number_results = len(results)
    logger.info(f"Extracted {number_results} records")
    load_status = helpers.load_violations(conn, cur, results, logger)
    logger.info(f"Load status: {load_status}")

    geocode_updates = []
    results = helpers.get_records2geocode(dict_cur, logger)
    number_results = len(results)
    logger.info(f"{number_results} records to GeoCode")
    for row in results:
        geocode_results = helpers.extract_geocodes(row, logger)
        if geocode_results:
            geocode_updates.append(geocode_results)
    logger.info('GeoCoding complete')

    logger.info(f'Number records {len(geocode_updates)} GeoCoded')

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

    logger = helpers.setup_logger_stdout('inspections_etl')

    process_inspections(logger)
