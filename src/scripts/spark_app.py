import sys
import logging
from abc import abstractclassmethod, ABC

from pyspark.sql import SparkSession

def get_spark():
    SparkSession.getOrCreate()

class SparkApp(ABC):
    def __init__(self, app_name, logger_name):
        self.app_name = app_name
        self.logger_name = logger_name

    @abstractclassmethod
    def run(self, spark, args):
        pass

    def main(self):
        # init spark
        spark = SparkSession\
            .builder.appName(f"{self.app_name}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()

        # init logger

        self.logger = logging
        # init args

        args = sys.argv[1:]

        self.logger.info(f'Starting app {self.app_name}')

        try:
            self.run(spark, args)
            self.logger.info(f'Finishing app {self.app_name}')
        except Exception as e:
            self.logger.info('Stoping app {self.app_name}')
            self.logger.exception(e)