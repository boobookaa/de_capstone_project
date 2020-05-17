# Spark driver for DataWarehouse processing
import logging
import logging.config
import configparser
from pathlib import Path

from pyspark.sql import SparkSession

from dwh_wrangling import DataWarehouseWrangling

config = configparser.ConfigParser()
config.read_file(open("review_project.cfg"))

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

def main():
    """
    Main function that start a process of DataWarehouse wrangling
    :return:
    """

    logging.debug("Start processing DataWarehouse")
    dwh_w = DataWarehouseWrangling()
    dwh_w.process_data()

#        dwh_w.process_stage()
#        dwh_w.process_dds()
#        dwh_w.process_dm_layer()


if __name__ == "__main__":
    main()