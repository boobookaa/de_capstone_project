# Spark driver for Storage files processing
import logging
import logging.config
import configparser
from pathlib import Path

from pyspark.sql import SparkSession

from source_data_wrangling import SourceDataWrangling


config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)


def create_sparksession():
    """
    Create a spark session
    """
    # return SparkSession.builder.master("local") \
    #     .appName("ReviewProject") \
    #     .master("local[4]") \
    #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
    #     .config("spark.hadoop.fs.s3a.access.key", config.get("AWS", "KEY")) \
    #     .config("spark.hadoop.fs.s3a.secret.key", config.get("AWS", "SECRET")) \
    #     .getOrCreate()
    return SparkSession.builder.master('yarn').appName("ReviewProject") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .enableHiveSupport().getOrCreate()

def main():
    """
    Main function that start a process of Storage files wrangling
    :return:
    """

    logging.debug("Initialization of a Spark session...")
    spark = create_sparksession()

    logging.debug("Start processing source data")
    sdw = SourceDataWrangling(spark)
    sdw.process_data()

if __name__ == "__main__":
    main()
