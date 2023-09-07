import os
import sys
import argparse
from typing import Dict, List
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Column
# from chispa.column_comparer import assert_column_equality


"""
Module Docstring

C:/Users/Martijn/miniconda3/python.exe 
"c:/Users/Martijn/Desktop/PySpark Exercise ABN Amro/join.py" 
--fpath_1 'data/dataset_one.csv' --fpath_2 'data/dataset_two.csv' --countries 'United Kingdom' 'Netherlands'

"""

__author__ = "Martijn Wesselius"


def parser():
    """ 
        Initialize parser
        Add arguments
        Read arguments from command line
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--fpath_1', type=str, required=True,
                        help='Path to first file')
    parser.add_argument('--fpath_2', type=str, required=True,
                        help='Path to second file')
    parser.add_argument('--countries', type=str, default=[], 
                        nargs='+', required=True,
                        help='Countries to filter from the data')
    args = parser.parse_args()

    check_fpaths(args)
    check_countries(args)

    return args


def check_fpaths(args):
    """ ... """

    current_path = os.getcwd()

    fpath_1 = os.path.join(current_path, args.fpath_1)
    if not os.path.exists(fpath_1):
        raise Exception('Path to first file does not exists!')
    
    fpath_2 = os.path.join(current_path, args.fpath_2)
    if not os.path.exists(fpath_2):
        raise Exception('Path to second file does not exists!')
       

def check_countries(args):
    """ ... """

    pd_df1 = pd.read_csv(args.fpath_1, header=0)
    all_countries = pd_df1['country'].unique()

    if not (set(args.countries).issubset(all_countries)):
        raise Exception('Specified countries are not (all) contained in data. \
                        Choose from France, Netherlands, United Kingdom, United States')


def filter(df: DataFrame, col_object: Column, values: List) -> DataFrame:
    """ ... """
    return df.filter(col_object.isin(values))


def rename(df: DataFrame, col_names: Dict) -> DataFrame:
    """ ... """

    for col in col_names.keys():
        df = df.withColumnRenamed(col, col_names[col]) 
    
    return df


def write_output(df: DataFrame):
    """ ... """  

    # df.write.mode('overwrite').format('csv').options(header='True', delimiter=',').csv("client_data/result.csv")
    current_path = os.getcwd()
    new_dir = 'client_data'
    new_path = os.path.join(current_path, new_dir)

    if not os.path.exists(new_path):
        os.mkdir(new_path)

    df.toPandas().to_csv('client_data/result.csv', 
                         header='True', index=False)


def main():
    """ Main entry point of the app """

    args = parser()

    # Create Spark session
    # master('local').appname('chispa').
    spark = SparkSession.builder.getOrCreate()



    # logging.basicConfig()
    # To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).


    # logger = logging.getLogger(__name__)



    log4jLogger = spark._jvm.org.apache.log4j
    log4jLogger.LogManager.getRootLogger().setLevel(log4jLogger.Level.DEBUG)
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")

    # print(log4jLogger.LogManager.getRootLogger().getLevel())
    # print(logger.getLevel())

    # handler = RotatingFileHandler('join.log', maxBytes=100, backupCount=3)
    # handler.setLevel(logging.DEBUG)

    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # handler.setFormatter(formatter)

    # logger.addHandler(handler)


    # sh = logging.StreamHandler(sys.stdout)
    # sh.setLevel(logging.DEBUG)
    # logger.addHandler(sh)

    # print(logger.handlers)



    # EXTRACT

    # Create Spark dataframes from csv
    df1 = spark.read.csv(args.fpath_1, header=True)
    df1 = df1.select(['id', 'email', 'country'])

    df2 = spark.read.csv(args.fpath_2, header=True)
    df2 = df2.select(['id', 'btc_a', 'cc_t'])
    

    # TRANSFORM

    # Join dataframes
    # assert_df_equality(df1, df2, ignore_row_order=True)
    df = df1.join(df2, on='id', how='leftouter')

    # Rename columns
    col_names = {"id":"client_identifier", 
                 "btc_a":"bitcoin_address", 
                 "cc_t":"credit_card_type"}
    df = rename(df, col_names)
    
    # Filter countries
    df = filter(df, df.country, args.countries)


    # LOAD

    # Write dataframe to csv
    # write_output(df)


    df.show()
    

if __name__ == "__main__":
    main()