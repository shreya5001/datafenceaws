import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, array, ArrayType, DateType
from pyspark.sql import Row, Column
import datetime
import json
import boto3
import logging
import calendar
import uuid
import time
from dateutil import relativedelta
from datetime import timedelta
import argparse

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv,['JOB_DATE', 'BRONZE_LAYER_NAMESPACE','S3_BUCKET', 'TABLES', 'SILVER_LAYER_NAMESPACE'])
print(args['JOB_DATE'])
print(args['BRONZE_LAYER_NAMESPACE'])
print(args['S3_BUCKET'])
print(args['TABLES'])


def merge_to_delta(BRONZE_TABLE_PATH, SILVER_TABLE_PATH, JOB_DATE, TABLE):
    try:
        deltaTable = DeltaTable.forPath(spark, SILVER_TABLE_PATH + "/" + "store_orders")
        if deltaTable:
            print("Delta table exists")
            df_read_data_incremental = spark.read                             \
                                            .option("header", "true")         \
                                            .option("inferSchema", "true")    \
                                            .csv(BRONZE_TABLE_PATH + "/" + JOB_DATE + "/" + "*.csv")
            df_read_data_incremental = df_read_data_incremental.withColumn("order_date", to_date(df_read_data_incremental.order_date,  'MM/dd/yyyy'))
            df_read_data_incremental = df_read_data_incremental.withColumn("updated_at", to_timestamp(df_read_data_incremental.updated_at,  'yyyy-MM-dd HH:mm:ss'))
            df_read_data_incremental.show(5)
            deltaTable.alias("store_orders").merge(
            df_read_data_incremental.alias("store_orders_incremental"),
                    "store_orders.order_number = store_orders_incremental.order_number")                     \
                    .whenMatchedUpdate(set = {"Op":         "store_orders_incremental.Op",                   \
                                              "order_number":     "store_orders_incremental.order_number",   \
                                              "customer_id":      "store_orders_incremental.customer_id",    \
                                              "product_id":       "store_orders_incremental.product_id",     \
                                              "order_date":       "store_orders_incremental.order_date",     \
                                              "units":            "store_orders_incremental.units",          \
                                              "sale_price":       "store_orders_incremental.sale_price",     \
                                              "currency":         "store_orders_incremental.currency",       \
                                              "order_mode":       "store_orders_incremental.order_mode",     \
                                              "updated_at":       "store_orders_incremental.updated_at"} )   \
                    .whenNotMatchedInsert(values =                                                           \
                       {                                                    
                                              "Op":         "store_orders_incremental.Op",                   \
                                              "order_number":   "store_orders_incremental.order_number",     \
                                              "customer_id":      "store_orders_incremental.customer_id",    \
                                              "product_id":       "store_orders_incremental.product_id",     \
                                              "order_date":       "store_orders_incremental.order_date",     \
                                              "units":            "store_orders_incremental.units",          \
                                              "sale_price":       "store_orders_incremental.sale_price",     \
                                              "currency":         "store_orders_incremental.currency",       \
                                              "order_mode":       "store_orders_incremental.order_mode",     \
                                              "updated_at":       "store_orders_incremental.updated_at"      \
                       }                                                                                     \
                     ).execute()
    except:
        print("Delta table does not exist")
        df_read_data_full = spark.read                          \
                                 .option("header", "true")      \
                                 .option("inferSchema", "true") \
                                 .csv(BRONZE_TABLE_PATH + "/" + "LOAD00000001.csv",schema=schema)
        
        df_read_data_full = df_read_data_full.withColumn("order_date", to_date(df_read_data_full.order_date,  'MM/dd/yyyy'))
        df_read_data_full = df_read_data_full.withColumn("updated_at", lit(current_timestamp()))
        PARTITION_COLUMN="currency"
        df_read_data_full.write.format("delta").option("path", SILVER_TABLE_PATH + "/" + "store_orders").partitionBy(PARTITION_COLUMN).saveAsTable("store_orders")
        df_read_data_full.show(5)
        
        


TABLE="store_orders"
BRONZE_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['BRONZE_LAYER_NAMESPACE'] + TABLE
print(BRONZE_TABLE_PATH)
SILVER_LAYER_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['SILVER_LAYER_NAMESPACE'] + TABLE

df_read_data_full = spark.read                                  \
                                 .option("header", "true")      \
                                 .option("inferSchema", "true") \
                                 .csv(BRONZE_TABLE_PATH + "/" + "LOAD00000001.csv")

display(df_read_data_full)
df_read_data_full.printSchema()
     



job.commit()