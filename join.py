import os
import sys
import argparse
from typing import Dict, List
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Column
# from chispa.column_comparer import assert_column_equality


__author__ = "Martijn Wesselius"


"""
    Setup global logger with rotating file strategy
"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = RotatingFileHandler('join.log', maxBytes=2000, backupCount=2)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


def parser():
    """ 
        Parser() initializes argument parser and checks if input is valid
    """

    logger.info("Argument parser start")

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
    logger.info("Parsed filepaths checked")
    check_countries(args)
    logger.info("Parsed countries checked")

    logger.info("Argument parser end")

    return args


def check_fpaths(args):
    """
        Checks if the parsed filepaths do exists
        Throws exception otherwise
    """
    
    current_path = os.getcwd()

    fpath_1 = os.path.join(current_path, args.fpath_1)
    if not os.path.exists(fpath_1):
        raise Exception('Invalid path to first file')
    
    fpath_2 = os.path.join(current_path, args.fpath_2)
    if not os.path.exists(fpath_2):
        raise Exception('Invalid path to second file')
       

def check_countries(args):
    """ 
        Checks if parsed countries are present in the data  
        Throws exception otherwise
    """

    pd_df1 = pd.read_csv(args.fpath_1, header=0)
    all_countries = pd_df1['country'].unique()

    if not (set(args.countries).issubset(all_countries)):
        raise Exception('Invalid countries selected, please choose from France, Netherlands, United Kingdom, United States')


def extract(spark, args) -> DataFrame:
    """
        Extract() reads CSV files from `args.fpath_1/2`
        to create Dataframes `df1` and `df2`
    """

    logger.info('Extraction start')

    df1 = spark.read.csv(args.fpath_1, header=True)
    df1 = df1.select(['id', 'email', 'country'])
    logger.info('DataFrame_1 extracted from CSV')

    df2 = spark.read.csv(args.fpath_2, header=True)
    df2 = df2.select(['id', 'btc_a', 'cc_t'])
    logger.info('DataFrame_2 extracted from CSV')

    logger.info('Extraction end')

    return df1, df2


def transform(df1: DataFrame, df2: DataFrame, args) -> DataFrame:
    """
        Transform() joins DataFrames `df1` and `df2` into 
        DataFrame `df`, renames columns of `df` 
        and filters its  rows
    """

    logger.info('Transformation start')

    col_names = {"id":"client_identifier", 
                 "btc_a":"bitcoin_address", 
                 "cc_t":"credit_card_type"}

    # assert_df_equality(df1, df2, ignore_row_order=True)
    df = df1.join(df2, on='id', how='leftouter')
    logger.info("DataFrames joined")
    
    df = rename(df, col_names)
    logger.info("DataFrames columns renamed")

    df = filter(df, df.country, args.countries)
    logger.info("DataFrames filtered on countries")

    logger.info('Transformation end')

    return df


def rename(df: DataFrame, col_names: Dict) -> DataFrame:
    """ 
        Renames DataFrame `df` accordng to names provided in `col_names`
    """

    for col in col_names.keys():
        df = df.withColumnRenamed(col, col_names[col]) 
    
    return df


def filter(df: DataFrame, col_object: Column, values: List) -> DataFrame:
    """
        Filters DataFrame `df` on the countries provided in `args.countries`
    """
    return df.filter(col_object.isin(values))


def save(df: DataFrame):
    """ 
        Save() writes DataFrame `df` to CSV file
    """  

    logger.info("Load start")

    current_path = os.getcwd()
    new_dir = 'client_data'
    new_path = os.path.join(current_path, new_dir)

    if not os.path.exists(new_path):
        os.mkdir(new_path)
        logger.info("Destination folder created")

    # df.write.mode('overwrite').format('csv').options(header='True', delimiter=',').csv("client_data/result.csv")
    df.toPandas().to_csv('client_data/result.csv', 
                         header='True', index=False)
    logger.info('DataFrame written to CSV')
    
    logger.info("Load end")


def main():
    """
        Main() creates argument parser, 
        initializes Spark session and 
        extracts, transforms and saves the data 
    """

    logger.info("Program start")

    # Parsed arguments
    try:
        args = parser()
    except Exception as e:
        print(e)
        logger.error(e)
        return

    # Create Spark session
    spark = SparkSession.builder.getOrCreate() # .master('local').appname('chispa')
    logger.info("Spark session created")

    # EXTRACT
    df1, df2 = extract(spark, args)

    # TRANSFORM
    df = transform(df1, df2, args)
    df.show()

    # LOAD
    save(df)

    logger.info("Program end")


if __name__ == "__main__":
    main()