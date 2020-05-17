# Process Reviews
import boto3
import configparser
import logging
import logging.config
from pathlib import Path

from pyspark.sql.functions import regexp_replace, col, upper

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

from helpers import product_working_zone_insert

class ReviewWrangling:
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
        self._file_mask = "review/parquet"
        self._s3_resource = boto3.resource("s3",
                                           region_name = config.get("AWS", "REGION"),
                                           aws_access_key_id = config.get("AWS", "KEY"),
                                           aws_secret_access_key = config.get("AWS", "SECRET")
                                           )
    def _get_files(self):
        """
        Get Reviews files from Landing Zone
        :return: list of files at the Landing Zone
        """
        return [obj.key for obj in self._s3_resource.Bucket(self._landing_zone).objects.filter(Prefix = self._file_mask)]

    def _process_customer(self, source_df, target_file):
        """
        Process Customer
        :param source_df: Spark DataFrame with source data
        :param target_file: name of the file for storing
        """
        customer_df = source_df.select("customer_id").filter("customer_id is not null").dropDuplicates()
        logging.debug(f"Writing {target_file}")
        customer_df.repartition(8).\
            write.csv(path = target_file, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')

    def _process_product(self, source_df, target_file):
        """
        Process Product
        :param source_df: Spark DataFrame with source data
        :param target_file: name of the file for storing
        """
        product_df = self._spark.sql(product_working_zone_insert)
        logging.debug(f"Writing {target_file}")
        product_df.coalesce(8).\
            write.csv(path = target_file, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')

    def _process_product_category(self, source_df, target_file):
        """
        Process Product Category
        :param source_df: Spark DataFrame with source data
        :param target_file: name of the file for storing
        """
        product_category_df = source_df.select("product_category").dropDuplicates().\
            withColumn("product_category_name", regexp_replace("product_category", '_', ' '))
        logging.debug(f"Writing {target_file}")
        product_category_df.coalesce(1).\
            write.csv(path = target_file, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')

    def _process_marketplace(self, source_df, target_file):
        """
        Process Marketplace
        :param source_df: Spark DataFrame with source data
        :param target_file: name of the file for storing
        """
        marketplace_df = source_df.select(upper(col("marketplace"))).dropDuplicates()
        logging.debug(f"Writing {target_file}")
        marketplace_df.coalesce(1).\
            write.csv(path = target_file, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')

    def _process_review(self, source_df, target_file):
        """
        Process Review
        :param source_df: Spark DataFrame with source data
        :param target_file: name of the file for storing
        """
        logging.debug(f"Writing {target_file}")
        source_df.repartition(8).\
            write.csv(path = target_file, sep = ';', header = True, mode = "overwrite", quote = '"', escape = '"')

    def process_data(self):
        """
        Run Reviews processing
        """
        if not len(self._get_files()) > 0:
            return
        src_review = f"s3a://{self._landing_zone}/{self._file_mask}"
        review_df = self._spark.read.parquet(src_review)

        # From a review data set will be retrieved data about these entities (dimensions and fact tables)
        _entities = {
            "marketplace":      {"process_func_name": self._process_marketplace, "create_view": False},
            "customer":         {"process_func_name": self._process_customer, "create_view": False},
            "product":          {"process_func_name": self._process_product, "create_view": True},
            "product_category": {"process_func_name": self._process_product_category, "create_view": False},
            "review":           {"process_func_name": self._process_review, "create_view": False}
        }

        for entity in _entities:
            logging.debug(f"Process {entity}...")
            if _entities[entity]["create_view"]:
                review_df.createOrReplaceTempView(entity)
            target_file = f"s3a://{self._working_zone}/{entity}"
            _entities[entity]["process_func_name"](review_df, target_file)