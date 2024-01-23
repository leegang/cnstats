#!/usr/bin/python3
# -*- coding: utf-8 -*- 
import duckdb
import pandas as pd
import os
import logging


def get_csv_file_paths(folder_path):
    csv_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                csv_file_paths.append(os.path.join(root, file))
    return csv_file_paths


def load_csv_to_duckdb(csv_file_path, table_name):
    df= pd.read_csv(csv_file_path,header=0)
    logger.info(f"正在入库{csv_file_path}")
    con = duckdb.connect(database='data/macrodb.duckdb',)
    con.register('my_table', df)
    # delete the table if it existsINSERT INTO hgnd_code (column0, column1)
    con.execute(f'DROP TABLE IF EXISTS {table_name}')
    con.execute(f'CREATE TABLE {table_name} AS SELECT id,name FROM my_table')
    con.commit() 
    con.close()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
# Specify the folder path
folder_path = 'data'

# Create the data folder if it doesn't exist
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# creat macrodata.duckdb file if it doesn't exist
if not os.path.exists('data/macrodb.duckdb'):
    con = duckdb.connect(database='data/macrodb.duckdb',)
    con.close()

# Get the csv file paths
csv_file_paths = get_csv_file_paths('hgnd')

# Load the code csv files to duckdb
load_csv_to_duckdb('hgnd/code.csv', 'hgnd_code')
logger.info(f"宏观年度数据code已经入库成功")

# Load the data csv files to duckdb
csv_nd_list = get_csv_file_paths('hgnd')

#csv_list remove the code.csv
csv_nd_list.remove('hgnd/code.csv')

#define a function to  Load the data csv files,concat them and then load to duckdb
def load_data_csv_list(csv_list,table_name):
    df_list = []
    for csv_file_path in csv_list:
        df = pd.read_csv(csv_file_path, header=None)
        df.columns = ['time_str', 'name', 'code', 'value']
        df_list.append(df)
    df = pd.concat(df_list,)
    con = duckdb.connect(database='data/macrodb.duckdb',)
    con.register('my_table', df)
    # delete the table if it exists
    con.execute(f'DROP TABLE IF EXISTS {table_name}')
    con.execute(f'CREATE TABLE {table_name} AS SELECT * FROM my_table')
    con.commit() 
    con.close()


load_data_csv_list(csv_nd_list,'hgnd_data')
logger.info(f"宏观年度数据已经入库成功")

csv_yd_list = get_csv_file_paths('hgyd')
csv_yd_list.remove('hgyd/code.csv')

load_csv_to_duckdb('hgyd/code.csv', 'hgyd_code')
logger.info(f"宏观月度数据code已经入库成功")

load_data_csv_list(csv_yd_list,'hgyd_data')
logger.info(f"宏观月度数据已经入库成功")


    




