# Processing Countries
import boto3
import configparser
import logging
import logging.config
from pathlib import Path

from pyspark.sql.types import StructField, StructType, StringType as Str, IntegerType as Int
from helpers import country_working_zone_insert

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

class CountryWrangling:
    def __init__(self, spark, landing_zone, working_zone):
        """
        Init function
        :param spark: SparkSession
        :param landing_zone: name of the S3 bucket for Landing Zone
        :param working_zone: name of the S3 bucket for Working Zone
        """
        self._spark = spark
        self._landing_zone = landing_zone
        self._working_zone = working_zone
        self._file_mask = "country/"
        self._s3_resource = boto3.resource("s3",
                                           region_name = config.get("AWS", "REGION"),
                                           aws_access_key_id = config.get("AWS", "KEY"),
                                           aws_secret_access_key = config.get("AWS", "SECRET")
                                           )
        self._country_data_schema = StructType([
            StructField('Continent_Name', Str(), True),
            StructField('Continent_Code', Str(), True),
            StructField('Country_Name', Str(), True),
            StructField('Two_Letter_Country_Code', Str(), True),
            StructField('Three_Letter_Country_Code', Str(), True),
            StructField('Country_Number', Int(), True)
        ])


    def _get_files(self):
        """
        Get Country files from Landing Zone
        :return: list of files at the Landing Zone
        """
        return [obj.key for obj in self._s3_resource.Bucket(self._landing_zone).objects.filter(Prefix = self._file_mask)]

    def process_data(self):
        """
        Process data
        """
        if not len(self._get_files()) > 0:
            return

        logging.debug("Process Country files...")
        src_country = f"s3a://{self._landing_zone}/{self._file_mask}"
        country_df = self._spark.read.json(src_country, schema = self._country_data_schema)
        country_df.createOrReplaceTempView("country")

        logging.debug("Clear Country data")
        country_cleared_df = self._spark.sql(country_working_zone_insert)

        tgt_country = f"s3a://{self._working_zone}/country"

        logging.debug(f"Writing {tgt_country}")
        country_cleared_df.coalesce(1).\
            write.csv(path = tgt_country, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')
