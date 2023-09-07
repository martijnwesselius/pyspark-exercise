import os
import argparse
from typing import Dict, List

import pyspark
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

    return args
    

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

    # check arguments

    # Create Spark session
    # master('local').appname('chispa').
    spark = SparkSession.builder.getOrCreate()

    # Create Spark dataframes from csv
    df1 = spark.read.csv(args.fpath_1, header=True)
    df1 = df1.select(['id', 'email', 'country'])

    df2 = spark.read.csv(args.fpath_2, header=True)
    df2 = df2.select(['id', 'btc_a', 'cc_t'])
    
    print(type(df1))
    print(type(df1.id))
    print(type(args))

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

    # Write dataframe to csv
    # write_output(df)


    df.show()


    # print(df.select('country').distinct().collect())
    # df.printSchema()

    

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()