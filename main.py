import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import re

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table


# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01" #????????

start_date_str = "2023-01-01"
end_date_str = "2025-11-01"

# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
print(dates_str_lst)




# # BRONZE =============================================================
# # create bronze datalake - lms
# bronze_lms_directory = "datamart/bronze/lms/"

# if not os.path.exists(bronze_lms_directory):
#     os.makedirs(bronze_lms_directory)

# # run bronze backfill - lms
# for date_str in dates_str_lst:
#     utils.data_processing_bronze_table.process_bronze_lms_table(date_str, bronze_lms_directory, spark)


# # bronze - clickstream
# bronze_clickstream_directory = "datamart/bronze/clickstream/"

# if not os.path.exists(bronze_clickstream_directory):
#     os.makedirs(bronze_clickstream_directory)

# start_date_str = "2023-01-01"
# end_date_str = "2024-12-01"
# dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
# print(dates_str_lst)

# for date_str in dates_str_lst:
#     utils.data_processing_bronze_table.process_bronze_clickstream_table(
#         date_str, 
#         bronze_clickstream_directory, 
#         spark
#     )


# # bronze - financials and attributes
# table_dict = {
#     'source': ['data/features_attributes.csv', 'data/features_financials.csv'],
#     'directory': ['datamart/bronze/attributes/', 'datamart/bronze/financials/'],
#     'filename': ['cust_attr', 'cust_fin']
#              }

# for i in range(len(table_dict['source'])):
#     if not os.path.exists(table_dict['directory'][i]):
#         os.makedirs(table_dict['directory'][i])
#     utils.data_processing_bronze_table.process_bronze_other_tables(
#         table_dict['source'][i], 
#         table_dict['directory'][i], 
#         table_dict['filename'][i], 
#         spark
#     )




# # SILVER =============================================================
# start_date_str = "2023-01-01"
# end_date_str = "2025-11-01"
# dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
# print("list of dates for lms:", dates_str_lst)

# silver_loan_daily_directory = "datamart/silver/loan_daily/"

# if not os.path.exists(silver_loan_daily_directory):
#     os.makedirs(silver_loan_daily_directory)
    
# dfs_full = []
# for date_str in dates_str_lst:
    
#     df_full, df_dropped = utils.data_processing_silver_table.process_silver_lms(
#         date_str, 'datamart/bronze/lms/', silver_loan_daily_directory, spark
#     )
    
#     dfs_full.append(df_full)

# # get loan_prod and loan_dim tables
# utils.data_processing_silver_table.process_silver_loan_prod(dfs_full, "datamart/silver/loan_product/")
# utils.data_processing_silver_table.process_silver_loan_dim(dfs_full, "datamart/silver/loan_dimensions/")



# # feature_attributes
# utils.data_processing_silver_table.process_silver_attr(
#     'datamart/bronze/attributes/bronze_cust_attr.csv', 
#     'datamart/silver/attributes/', 
#     spark
# )


# # feature_financials
# utils.data_processing_silver_table.process_silver_fin(
#     'datamart/bronze/financials/bronze_cust_fin.csv', 
#     'datamart/silver/financials/', 
#     spark
# )


# # feature_clickstreams
# start_date_str = '2023-01-01'
# end_date_str = '2024-12-01'

# dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
# print("list of dates for clickstream:", dates_str_lst)

# bronze_clickstream_directory = 'datamart/bronze/clickstream/'
# silver_clickstream_directory = 'datamart/silver/clickstream/'

# if not os.path.exists(silver_clickstream_directory):
#     os.makedirs(silver_clickstream_directory)

# for date_str in dates_str_lst:
#     utils.data_processing_silver_table.process_silver_clickstream(
#         date_str, 
#         bronze_clickstream_directory, 
#         silver_clickstream_directory, 
#         spark
#     )



# GOLD =============================================================
# create gold datalake - lms
gold_label_store_directory = "datamart/gold/label_store/"

if not os.path.exists(gold_label_store_directory):
    os.makedirs(gold_label_store_directory)

# run gold backfill - lms
for date_str in dates_str_lst:
    utils.data_processing_gold_table.process_labels_gold_table(date_str, silver_loan_daily_directory, gold_label_store_directory, spark, dpd = 30, mob = 6)


folder_path = gold_label_store_directory
files_list = [folder_path+os.path.basename(f) for f in glob.glob(os.path.join(folder_path, '*'))]
df = spark.read.option("header", "true").parquet(*files_list)
print("row_count:",df.count())

df.show()



    