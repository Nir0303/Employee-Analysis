# /usr/bin/env python
"""
Main module for spark application
"""
from __future__ import print_function
from __future__ import division

import re
import os
import configparser
import argparse
import logging
import datetime
import requests
from pyspark.sql.functions import lit, collect_list, concat_ws, concat

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import StorageLevel
from utility import create_directory, remove_directory


config = configparser.ConfigParser()
config.read("variables.ini")

COGO_LABS_JDBC_USER_NAME = config["DEFAULT"]["cogo_labs_jdbc_user_name"]

DATA_PATH = os.path.join(os.getcwd(), "data")
LIVE_WORKS_JDBC_URL = config["DEFAULT"]["live_works_jdbc_url"]
COGO_LABS_URL = config["DEFAULT"]["cogo_labs_api_url"]
COGO_LABS_JDBC_PASSWORD = config["DEFAULT"]["cogo_labs_jdbc_password"]

#fetching 10 rows with each spark task
PROPERTIES = {
    "user": COGO_LABS_JDBC_USER_NAME,
    "password": COGO_LABS_JDBC_PASSWORD,
    "driver": 'com.mysql.jdbc.Driver',
    "fetchsize": "10"
}


def generate_cogo_labs_data():
    """
    Generate json file from cogo labs api
    :return:

    Parse json file to form array of json
    api response
    {"rows":[{"id":1,"name":"Vanessa Thomas","address":"87826 Ford Canyon Reeseport, WI 70426","birthdate":"1971-02-22","sex":"F","job":
    "Advice worker","company":"May, Lee and Jackson","emd5":"00001552528'}],"num_rows":1}

    Parse data and cleanse it as array of json.

    [{"id":1,"name":"Vanessa Thomas","address":"87826 Ford Canyon Reeseport, WI 70426","birthdate":"1971-02-22","sex":"F","job":
    "Advice worker","company":"May, Lee and Jackson","emd5":"00001552528'}]

    """
    response = requests.get(COGO_LABS_URL)
    response_metadata_regex = re.compile(r"\"num_rows\":\d*\}")
    with open("data/cogo_chunks.txt", "wb") as f:
        # reading data of 128 bytes from api
        for index, chunk in enumerate(response.iter_content(chunk_size=128)):
            if index == 0:
                #remove rows key-value pair to make json array for spark
                chunk = re.sub(r"\{\"rows\":", "", chunk)
            if response_metadata_regex.search(chunk):
                chunk = response_metadata_regex.sub("", chunk)
            f.write(chunk)


def parse_args():
    """
        function for argument parsing
        :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", "-c", help="Cache input from liveworks and  cogo labs", action='store_true')
    parser.add_argument("--limit", "-d", help="Limit data from live works and cogo labs", type=int)
    parser.add_argument("--log_level", "-l", help="Set loglevel for debugging and analyis",
                         default="INFO")
    parser.add_argument("--master", "-m", help="Set master for spark application", default="local[*]")
    args = parser.parse_args()
    return args


class App(object):
    """
    Application for cogo labs assessment
    """

    def __init__(self, args=None):
        """
        instantiate App
        :param args: command line arugements
        """
        self.args = args
        master = self.args.master if args else "local[*]"
        # pass mysql drivers to spark executor and driver for execution

        self.spark = SparkSession.builder \
            .appName("Cogo labs assessment test") \
            .config("spark.driver.extraClassPath", "mysql-connector-java-5.1.45-bin.jar") \
            .config("spark.executor.extraClassPath", "mysql-connector-java-5.1.45-bin.jar") \
            .config("spark.sql.warehouse.dir", "spark-warehouse/") \
            .master(master) \
            .getOrCreate()
        self._liveworks_df = None
        self._cogo_labs_df = None
        self._join_df = None

    @property
    def live_works_df(self):
        """
        Create live works dataframe from jdbc connection or cached data
        :return:
        """
        logger.info("Creating live works data frame")
        if self.args.cache and os.path.exists("spark-warehouse/liveworks"):
            logger.info("Creating liveworks dataframe from cached data")
            self._liveworks_df = self.spark.read.parquet("spark-warehouse/liveworks/*.parquet")
        else:
            # read from jdbc url
            self._liveworks_df = self.spark.read.jdbc(LIVE_WORKS_JDBC_URL, table="cogo_list_v1", properties=PROPERTIES)
            # TODO:
            # add refresh option to reload cache data
            if self.args.cache:
                # cache data for future use
                logger.info("Caching live works data for next run")
                self._liveworks_df.write.saveAsTable("liveworks")

        return self._liveworks_df.limit(self.args.limit) if self.args.limit else self._liveworks_df

    @property
    def cogo_labs_df(self):
        """
        Create cogo labs data frame from json data or cached data
        :return:
        """
        logger.info("Creating cogo labs data frame")
        if self.args.cache and os.path.exists("spark-warehouse/cogolabs"):
            logger.info("Creating cogolabs dataframe from cached data")
            self._cogo_labs_df = self.spark.read.parquet("spark-warehouse/cogolabs/*.parquet")
        else:
            # get / download data from cogo labs api
            generate_cogo_labs_data()
            columns = "address birthdate company emd5 id job name sex"
            # Generate schema
            cogo_labs_schema = StructType(
                [StructField(column, StringType(), True) for column in columns.split(" ")])
            # create cogo labs dataframe using schema
            self._cogo_labs_df = self.spark.read.json("data/cogo_chunks.txt", cogo_labs_schema)
            self._cogo_labs_df = self._cogo_labs_df.toDF(
                *("cogo_" + str(column) for column in self._cogo_labs_df.columns))
            # TODO:
            # add refresh option to reload cache data
            if self.args.cache:
                logger.info("Caching cogo labs data for next run")
                self._cogo_labs_df.write.saveAsTable("cogolabs")
        return self._cogo_labs_df.limit(self.args.limit) if self.args.limit else self._cogo_labs_df

    @property
    def join_df(self):
        """
        Create join data frame from cogo labs and live works dataframe joined on emd5
        :return:
        """
        logger.info("Creating Join dataframe from cogo labs and live works data frame")
        cogo_labs_df = self.cogo_labs_df
        live_works_df = self.live_works_df
        # create join dataframe
        self._join_df = cogo_labs_df.join(live_works_df, live_works_df.emd5 == cogo_labs_df.cogo_emd5, "fullouter") \
            .select(live_works_df.emd5, live_works_df.name, live_works_df.job, live_works_df.company
                     , cogo_labs_df.cogo_emd5, cogo_labs_df.cogo_name, cogo_labs_df.cogo_job,
                     cogo_labs_df.cogo_company)
        return self._join_df

    def persist(self):
        """
        persist intermediate dataframe for better performance
        :return:
        """
        logger.info("Persist dataframe for better performance")
        self._liveworks_df.persist(StorageLevel.DISK_ONLY)
        self._cogo_labs_df.persist(StorageLevel.DISK_ONLY)
        self._join_df.persist(StorageLevel.DISK_ONLY)

    def unpersist(self):
        """
        unpersist dataframes
        :return:
        """
        logger.info("unpersist intermediate dataframes")
        self._liveworks_df.unpersist()
        self._cogo_labs_df.unpersist()
        self._join_df.unpersist()

    def run(self):
        """
        Run application
        :return:
        """
        join_df = self.join_df
        self.persist()

        # Create dataframes for intersection , cogo labs and liveworks only dataframes
        logger.info(" Creating common dataframe emd5 present both in live works and cogo labs")
        """
        Cogo labs
        emd5    Name
        1       Sam
        2       Henry
        
        Liveworks
        emd5    Name
        2       John
        3       Smith
        
        Full Outer Join
        c_emd5    l_emd5   c_name  l_name
        1           Null     Sam     Null
        2           2       Henry   John
        Null        3        Null    Smith
        
        Intersection from cogo labs and Live works, where c_emd5 and l_emd5 is not null
        c_emd5    l_emd5   c_name  l_name
        2           2       Henry   John   
        
        Users only from cogo labs, where c_emd5 is not null and l_emd5 is null
        c_emd5    l_emd5   c_name  l_name
        1           Null     Sam     Null
        
        Users only from live works, where l_emd is not null and c_emd5 is null
        c_emd5    l_emd5   c_name  l_name
        Null        3        Null    Smith
        
        """

        common_df = join_df.filter(~join_df.emd5.isNull() & ~join_df.cogo_emd5.isNull())
        logger.info(" Creating cogo labs only dataframe where emd5 present in cogo labs and not present in live works")
        cogo_labs_only_df = join_df.filter(~join_df.cogo_emd5.isNull() & join_df.emd5.isNull())
        logger.info(" Creating live works only dataframe where emd5 present in liveworks and not present in cogo labs")
        live_works_only_df = join_df.filter(join_df.cogo_emd5.isNull() & ~join_df.emd5.isNull())

        # counting distinct emd5 counts
        intersection_count = common_df.select(common_df.cogo_emd5).distinct().count()
        cogo_labs_only_count = cogo_labs_only_df.select(cogo_labs_only_df.cogo_emd5).distinct().count()
        live_works_only_count = live_works_only_df.select(live_works_only_df.emd5).distinct().count()

        logger.info("Number of Unique users present in both cogo labs and liveworks %s", intersection_count)
        logger.info("Number of Unique users present only in cogo labs data %s", cogo_labs_only_count)
        logger.info("Number of Unique users present only in live works data %s", live_works_only_count)

        # Create common job data frame with users having same job title
        logger.info("Creating common job data frame where common users have same job title")
        common_job_df = common_df.where(common_df.cogo_job == common_df.job)
        common_job_df.persist(StorageLevel.DISK_ONLY)
        print("Output with common emd5 users having same job title")
        common_job_df.show()
        common_job_count = common_job_df.count()

        # Calculate percentage common emd5 users have different job titles
        different_jobs_percent = ((intersection_count - common_job_count) / intersection_count) * 100
        logger.info("Number of  users with common job present in both cogo labs and liveworks %s",
                    common_job_df.count())
        logger.info("Percent have different job titles in intersection %s", different_jobs_percent)

        # jsonsify data from common data frame
        """
        Create Key:Value pair
        Key = Job title , Value = Company Name
        
        cogolabs_emd5   cogolabs_job    cogolabs_company    liveworks_job   liveworks_company
        1              Hotel manager     Bender PLC          Barrister           Brown PLC
        1            Immigration officer Diaz Ltd                                           
        
        cogolabs_emd5   cogo_labs_c                         liveworks_c
        1               {"Hotel manager":"Bender PLC"}      {"Barrister":"Brown PLC"}
        1               {"Immigration officer":"Diaz Ltd"} 
        """

        common_json_df = common_df.withColumn("live_works_c", concat(lit("{\""), common_df.job, lit("\":\""),
                                                                     common_df.company, lit("\"}"))) \
            .withColumn("cogo_labs_c", concat(lit("{\""), common_df.cogo_job,
                                              lit("\":\""), common_df.cogo_company, lit("\"}")))

        """
        Concatenate results to form Array of key value pairs group by emd5
        emd5            cogolabs_json                                               liveworks_json
        1   [{"Hotel manager":"Bender PLC"},{"Immigration officer":"Diaz Ltd"}]     [{"Barrister":"Brown PLC"}]

        """

        common_agg_df = common_json_df.select(common_json_df.emd5, common_json_df.cogo_labs_c,
                                               common_json_df.live_works_c) \
            .groupBy(common_json_df.emd5).agg(concat_ws(",", collect_list(common_json_df.cogo_labs_c)),
                                                concat_ws(",", collect_list(common_json_df.live_works_c))) \
            .withColumnRenamed("concat_ws(,, collect_list(cogo_labs_c))", "cogo_labs_json") \
            .withColumnRenamed("concat_ws(,, collect_list(live_works_c))", "live_works_json")

        final_df = common_agg_df.withColumn("cogo_labs_json",
                                             concat(lit("["), common_agg_df.cogo_labs_json, lit("]"))) \
            .withColumn("live_works_json", concat(lit("["), common_agg_df.live_works_json, lit("]")))
        print("Final output with emd5 , cogolabs json and liveworks json")
        final_df.persist(StorageLevel.DISK_ONLY)
        final_df.show()

        # save final output as csv

        if os.path.exists("data/final_output/"):
            remove_directory("data/final_output/")
        logger.info("Save final output as csv")
        final_df.repartition(1).write.format("csv").save(os.path.join(DATA_PATH, "final_output"))


        self.unpersist()


if __name__ == '__main__':
    START_TIME = datetime.datetime.now()
    logger = logging.getLogger("Cogo labs assessment")  # pylint: disable=C0103
    create_directory("logs/")
    create_directory("data/")
    file_handler = logging.FileHandler("logs/main.log", mode="w")  # pylint: disable=C0103
    format_handler = logging.Formatter(fmt="%(asctime)s - %(name)s - "
                                       "%(levelname)s - %(message)s")  # pylint: disable=C0103
    file_handler.setFormatter(format_handler)
    logger.addHandler(file_handler)
    args = parse_args()  # pylint: disable=C0103
    if args.log_level == "INFO":
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    try:
        logger.info("Process started at %s", START_TIME)
        logger.info("Creating application instance")
        app = App(args)
        logger.info("Running application")
        app.run()
        END_TIME = datetime.datetime.now()
        logger.info("Application completed successfully at %s", END_TIME)
        logger.info("Total run time %s", END_TIME-START_TIME)
    except Exception as exp:
        logger.error("Application failed because of %s", exp)
