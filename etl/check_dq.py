# Data Quality

import configparser
import psycopg2
import logging
import logging.config
from pathlib import Path
import json

from helpers.check_data_quality_config import *

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

class CheckDataQuality:
    def __init__(self):
        """
        Init function
        """
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()

    def _exec_query(self, check_query):
        """
        Execute queries like as select count(*)
        :param check_query:
        :return: number of records
        """
        logging.debug(f"Execute query: {check_query}")
        self._cur.execute(check_query)
        self._conn.commit()
        return self._cur.fetchone()

    def _write_check_result(self, result_query):
        """
        Write a checking result in the etl.check_dq table
        :param result_query:
        :return:
        """
        logging.debug(f"Write result to check_dq table: {result_query}")
        self._cur.execute(result_query)
        self._conn.commit()

    def check_data_quality(self):
        """
        Main function that start an iteration process of checking data quality
        :return:
        """
        for check in check_dq_type:
            query_template = check["query_template"]

            for obj in check["check_object"]:
                check_query = query_template.format(**obj)
                result = self._exec_query(check_query)
                if result[0] > 0:
                    param_json = json.dumps(obj)
                    insert_check_dq = check_dq_insert.format(**check, query = check_query, param = param_json, row_number = result[0])
                    self._write_check_result(insert_check_dq)