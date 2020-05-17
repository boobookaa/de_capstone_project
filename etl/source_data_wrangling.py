# Main class for data procesing
import boto3
import configparser
import logging
import logging.config
from pathlib import Path

from country_wrangling import CountryWrangling
from review_wrangling import ReviewWrangling

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

class SourceDataWrangling:
    def __init__(self, spark):
        """
        Init function
        :param spark: SparkSession
        """
        self._spark = spark
        self._s3_resource = boto3.resource("s3",
                                           region_name = config.get("AWS", "REGION"),
                                           aws_access_key_id = config.get("AWS", "KEY"),
                                           aws_secret_access_key = config.get("AWS", "SECRET")
                                           )
        self._landing_zone = config.get("S3_STORAGE", "LANDING_ZONE")
        self._working_zone = config.get("S3_STORAGE", "WORKING_ZONE")

    def process_data(self):
        """
        Process source data at the landing Zone
        """
        logging.debug("Start process Landing Zone...")

        if self.is_landing_zone_empty():
            logging.debug("Landing Zone is empty, nothing to process")
            return

        if not self.is_working_zone_empty():
            logging.debug("Working Zone is not empty. Skip processing Landing Zone and go to process DWH")
            return

        # Process Country files
        logging.debug("Start process Country files")
        country = CountryWrangling(self._spark, self._landing_zone, self._working_zone)
        country.process_data()

        # Process Review files
        logging.debug("Start process Review files")
        review = ReviewWrangling(self._spark, self._landing_zone, self._working_zone)
        review.process_data()

        logging.debug("Delete objects from Landing Zone after processing")
        self._clear_bucket(self._landing_zone)

    def is_landing_zone_empty(self):
        """
        Check files existing at Landing Zone
        :return: True if files exist
        """
        return self._is_bucket_empty(self._landing_zone)

    def is_working_zone_empty(self):
        """
        Check files existing at Working Zone
        :return: True if files exist
        """
        return self._is_bucket_empty(self._working_zone)

    def _is_bucket_empty(self, bucket_name):
        """
        Check files existing in a specified bucket
        :param bucket_name:
        :return: True if files exist
        """
        return (lambda x: False if x > 0 else True)(len([obj.key for obj in self._s3_resource.Bucket(bucket_name).objects.all()]))

    def _clear_bucket(self, bucket_name):
        """
        Clear bucket
        :param bucket_name: name of the bucket that should be cleaned
        """
        self._s3_resource.Bucket(bucket_name).objects.all().delete()
