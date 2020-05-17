import boto3
import configparser
import psycopg2
import logging
import logging.config
from pathlib import Path

from check_dq import CheckDataQuality

from helpers import truncate_stg_table_queries, sql_stg_copy_template, stg_copy_object, \
    upsert_dds_table_queries, upsert_dm_table_queries

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

class DataWarehouseWrangling:
    def __init__(self):
        """
        Init function
        """
        self._region = config.get("AWS", "REGION")
        self._working_zone = config.get("S3_STORAGE", "WORKING_ZONE")
        self._iam_role_arn = config.get("IAM_ROLE", "ROLE_ARN")
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()
        self._s3_resource = boto3.resource("s3",
                                           region_name = config.get("AWS", "REGION"),
                                           aws_access_key_id = config.get("AWS", "KEY"),
                                           aws_secret_access_key = config.get("AWS", "SECRET")
                                           )

    def _exec_query(self, query_list):
        """
        Execute queries from a list one by one
        :param query_list: list of queries
        """
        for query in query_list:
            logging.debug(f"Execute query: {query}")
            self._cur.execute(query)
            self._conn.commit()

    def _is_object_exist(self, key):
        """
        Check particular file existing at the Working Zone
        :param key: S3 bucket object
        :return: True if key exists, False if key doesn't exists
        """
        logging.debug(f"Check {key} at Storing Layer")
        return (lambda x: True if x > 0 else False)(len([obj.key for obj in self._s3_resource.Bucket(self._working_zone).objects.filter(Prefix = key)]))

    def _clear_working_zone(self):
        """
        Clear Working Zone after processing all the data
        """
        self._s3_resource.Bucket(self._working_zone).objects.all().delete()

    def _is_data_ready(self):
        """
        All the data at the Working Zone should be processed.
        :return: True if data are ready, False vice versa
        """
        logging.debug("Check data at Storing Layer")
        return (lambda x: True if x > 0 else False)(len([obj.key for obj in self._s3_resource.Bucket(self._working_zone).objects.all()]))

    def _process_stage(self):
        """
        Fill in the tables in the STG schema using specified queries
        """
        logging.debug("Process Stage Layer")
        # truncate stage tables
        self._exec_query(truncate_stg_table_queries)

        # Set parameters for queries
        copy_query_list = []
        for obj in stg_copy_object:
            if self._is_object_exist(obj["key"]):
                copy_query = sql_stg_copy_template.format(**obj, bucket_name = self._working_zone, iam_role = self._iam_role_arn, region_name = self._region)
                copy_query_list.append(copy_query)
        if len(copy_query_list) > 0:
            self._exec_query(copy_query_list)

    def _process_dds(self):
        """
        Fill in the tables in the DDS schema using specified queries
        """
        logging.debug("Process DDS Layer")
        self._exec_query(upsert_dds_table_queries)

    def _process_dm(self):
        """
        Fill in the tables in the DM schema using specified queries
        """
        logging.debug("Process DataMarts")
        self._exec_query(upsert_dm_table_queries)

        logging.debug("Delete all objects from Working Zone")
        self._clear_working_zone()

    def _check_data_quality(self):
        """
        Check data quality after all processings
        :return:
        """
        logging.debug("Start checking data quality")
        check_dq = CheckDataQuality()
        check_dq.check_data_quality()

    def process_data(self):
        """
        Start all the processes of data transformation in the DataWarehouse
        :return:
        """
        logging.debug("Check data at Working Zone")
        if not self._is_data_ready():
            logging.debug("Working Zone is empty, nothing to process")
            return

        self._process_stage()
        self._process_dds()
        self._process_dm()
        self._check_data_quality()


