# Setup Python and Spark libraries
import sys
from awsglue.transforms import *
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as fn
from urllib.request import urlopen
from pyspark.sql.functions import udf
import hashlib
import urllib.request
from io import StringIO

from delta.tables import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, array, ArrayType, DateType, TimestampType, FloatType
import json

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
#spark = glueContext.spark_session
spark = SparkSession \
            .builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

job = Job(glueContext)
# Send arguments to the job

sys.argv+=["--S3_BUCKET", "aws-analytics-course"]
sys.argv+=["--BRONZE_LAYER_NAMESPACE", "bronze/dms/sales"]
sys.argv+=["--BRONZE_LAYER_ECOMMERCE_NAMESPACE", "bronze/kinesis"]
sys.argv+=["--SILVER_LAYER_NAMESPACE", "silver"]
sys.argv+=["--JOB_DATE", "2023/01/12/18"]
sys.argv+=["--TABLES", "{\"store_orders\": \"currency\", \"store_customers\": \"country\", \"products\": \"product_category\"}"]
sys.argv+=["--JOIN_COLUMNS_DELTA", "{\"store_orders\": \"order_number\", \"store_customers\": \"email\",  \"products\": \"product_id\"}"]
sys.argv+=["--JOIN_COLUMNS_INCREMENTAL", "{\"store_orders\": \"order_number\", \"store_customers\": \"email\", \"products\": \"product_category\"}"]
sys.argv+=["--CURRENCY_CONVERSION_URL", "https://api.exchangerate-api.com/v4/latest/usd"]
sys.argv+=["--ECOMMERCE_LOGS_BUCKET", "aws-analytics-incoming"]
sys.argv+=["--ECOMMERCE_STREAM_DATE", "2023/01/31/17"]
sys.argv+=["--GLUE_SILVER_DATABASE", "electroniz_curated"]

args = getResolvedOptions(sys.argv,["S3_BUCKET", "BRONZE_LAYER_NAMESPACE", "BRONZE_LAYER_ECOMMERCE_NAMESPACE", "SILVER_LAYER_NAMESPACE", "JOB_DATE", "TABLES", "JOIN_COLUMNS_DELTA", "JOIN_COLUMNS_INCREMENTAL", "CURRENCY_CONVERSION_URL", "ECOMMERCE_LOGS_BUCKET", "ECOMMERCE_STREAM_DATE", "GLUE_SILVER_DATABASE"])
# Get data from currency conversion API

currency_conversion_response = urlopen(args['CURRENCY_CONVERSION_URL'])
currency_conversion_json = json.loads(currency_conversion_response.read())
# Function to get currency conversion rates

def get_currency_conversion(currency_conversion_json, currency):
    return currency_conversion_json['rates'][currency]
# Function to covert currency to USD

def curate_sales_price(currency, sales_price):
  if (currency != 'USD'):
    curated_value = float(sales_price)/float(get_currency_conversion(currency_conversion_json, currency))
    return float(curated_value)
  else:
    return float(sales_price)
curate_sales_price_udf = udf(curate_sales_price, FloatType())
# Function to mask PII data

def mask_value(column):
  mask_value = hashlib.sha256(column.encode()).hexdigest()
  return mask_value

mask_udf = udf(mask_value, StringType())
# Function to convert IP octect

def ip_to_country(ip):
  ipsplit = ip.split(".")
  ip_number=16777216*int(ipsplit[0]) + 65536*int(ipsplit[1]) + 256*int(ipsplit[2]) + int(ipsplit[3])  
  return ip_number

ip_to_country_udf = udf(ip_to_country, StringType())
# Function to read data from S3 and create a Spark dataframe

def get_dataframe(BRONZE_TABLE_PATH):
    try:
        df_read_data_incremental = spark.read                             \
                                            .option("header", "true")         \
                                            .option("inferSchema", "true")    \
                                            .csv(BRONZE_TABLE_PATH)

        df_read_data_incremental=curate_columns(df_read_data_incremental, 'S')
        df_read_data_incremental = df_read_data_incremental.drop('Op')
        df_read_data_incremental.printSchema()
        df_read_data_incremental.show(10)
        return df_read_data_incremental
    except:
        return 0
    
# Function to merge data in a Delta Lake table

def merge_to_delta(SILVER_TABLE_PATH, BRONZE_TABLE_PATH, JOB_DATE, JOIN_COLUMN_DELTA, JOIN_COLUMN_INCREMENTAL):
    DELTA_TABLE_ALIAS="delta_table"
    INCREMENTAL_TABLE_ALIAS="data_incremental"
    JOIN_CONDITION=DELTA_TABLE_ALIAS + "." + JOIN_COLUMN_DELTA + "=" + INCREMENTAL_TABLE_ALIAS + "." + JOIN_COLUMN_INCREMENTAL
    df_read_data_incremental = get_dataframe(BRONZE_TABLE_PATH + "/" + JOB_DATE + "/" + "*.csv")
    if df_read_data_incremental != 0:
        deltaTable = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
        if deltaTable:
            print("Delta table exists")
            deltaTable.alias(DELTA_TABLE_ALIAS).merge(
                    source=df_read_data_incremental.alias(INCREMENTAL_TABLE_ALIAS),
                    condition=fn.expr(JOIN_CONDITION)) \
                    .whenMatchedUpdateAll()            \
                    .whenNotMatchedInsertAll()         \
                    .execute()
# Function to create a delta lake table

def merge_data_to_delta(BRONZE_TABLE_PATH, SILVER_TABLE_PATH, JOB_DATE, TABLE, PARTITION_COLUMN, JOIN_COLUMN_DELTA, JOIN_COLUMN_INCREMENTAL):
    try:   
        deltaTable = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
        if deltaTable:
            print("Delta table exists")
            merge_to_delta(SILVER_TABLE_PATH, BRONZE_TABLE_PATH, JOB_DATE, JOIN_COLUMN_DELTA, JOIN_COLUMN_INCREMENTAL)
    except:
        print("Delta table does not exist")
        df_read_data_full = get_dataframe(BRONZE_TABLE_PATH + "/" + "LOAD00000001.csv")
        df_read_data_full.write.format("delta").save(SILVER_TABLE_PATH)
        merge_to_delta(SILVER_TABLE_PATH, BRONZE_TABLE_PATH, JOB_DATE, JOIN_COLUMN_DELTA, JOIN_COLUMN_INCREMENTAL)
        deltaTable = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
        deltaTable.generate("symlink_format_manifest")
# Function to covert email to lowercase

def curate_email(email):
  curated_value = email.lower()
  return curated_value

curate_email_udf = udf(curate_email, StringType())
# Function to covert countries to abbreviated version

def curate_country(country):
  if (country == 'USA' or country == 'United States'):
    curated_value = 'USA'
  elif (country == 'UK' or country == 'United Kingdom'):
    curated_value = 'UK'
  elif (country == 'CAN' or country == 'Canada'):
    curated_value = 'CAN'
  elif (country == 'IND' or country == 'India'):
    curated_value = 'IND'
  else:
    curated_value = country
  return curated_value

curate_country_udf = udf(curate_country, StringType())
# Function to get curate columns

def curate_columns(df, mode):
    if ("order_date" in df.columns) and mode=='S':
        df = df.withColumn("order_date", to_date(df.order_date,  'MM/dd/yyyy'))
    if ("order_date" in df.columns) and mode=='E':
        df = df.withColumn("order_date", to_date(df.order_date,  'dd/MM/yyyy'))
    if "updated_at" in df.columns:
        df = df.withColumn("updated_at", to_timestamp(df.updated_at,  'yyyy-MM-dd HH:mm:ss')) 
    if "sale_price" in df.columns:
        df = df.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'sale_price'))
    if "email" in df.columns:
        df = df.withColumn('email_curated',curate_email_udf('email')).drop('email').withColumnRenamed('email_curated', 'email')
    if "country" in df.columns:
        df = df.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
    if "phone" in df.columns:
        df = df.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
    if "city" in df.columns:
        df = df.withColumn('city_masked',mask_udf('city')).drop('city').withColumnRenamed('city_masked', 'city')
    if "postalcode" in df.columns:
        df = df.withColumn('postalcode_masked',mask_udf('postalcode')).drop('postalcode').withColumnRenamed('postalcode_masked', 'postalcode')        
    if "credit_card" in df.columns:
        df = df.withColumn("credit_card",df.credit_card.cast(StringType()))
        df = df.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
    if "address" in df.columns:
        df = df.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
    return df
# Function to flatten JSON files

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [fn.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df
# Function to create glue tables

def create_glue_table(TABLE, BUCKET, SILVER_LAYER_NAMESPACE, GLUE_SILVER_DATABASE, COLUMNS):
    TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['SILVER_LAYER_NAMESPACE']  + "/" +TABLE
    table_sql=f"""
             CREATE EXTERNAL TABLE IF NOT EXISTS """ + args['GLUE_SILVER_DATABASE'] + """.""" + TABLE + """ 
             ( """ + COLUMNS + """)
              ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
              STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
              LOCATION '""" + TABLE_PATH +  """/_symlink_format_manifest/'
      """
    #print(table_sql)
    spark.sql(table_sql)
# Function to read raw data fro bronze layer and merge data for SALES tables

TABLE_DICT = json.loads(args['TABLES'])
JOIN_COLUMNS_DELTA_DICT = json.loads(args['JOIN_COLUMNS_DELTA'])
JOIN_COLUMNS_INCREMENTAL_DICT = json.loads(args['JOIN_COLUMNS_INCREMENTAL'])

for TABLE in TABLE_DICT:
    BRONZE_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['BRONZE_LAYER_NAMESPACE'] + "/" + TABLE
    SILVER_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['SILVER_LAYER_NAMESPACE'] + "/" + TABLE
    merge_data_to_delta(BRONZE_TABLE_PATH, SILVER_TABLE_PATH, args['JOB_DATE'], TABLE, TABLE_DICT[TABLE],  JOIN_COLUMNS_DELTA_DICT[TABLE], JOIN_COLUMNS_INCREMENTAL_DICT[TABLE])
# Read ecommerce logs from bronze layer and map IP addresses to countries  

IPLOCATION_SCHEMA =[
    ('ip1', IntegerType()),
    ('ip2', IntegerType()),
    ('country_code', StringType()),
    ('country_name', StringType())
]

ipfields = [StructField(*field) for field in IPLOCATION_SCHEMA]
schema_iplocation = StructType(ipfields)

IPLOCATION_PATH="s3a://" + args['ECOMMERCE_LOGS_BUCKET'] + "/" + "iplocation/"
df_iplocation = spark.read.csv(IPLOCATION_PATH, schema=schema_iplocation)
df_iplocation.registerTempTable('iplocation')

LOGS_SCHEMA =[
    ('time', StringType()),
    ('remote_ip', StringType()),
    ('country_name', StringType()),
    ('ip_number', IntegerType()),
    ('request', StringType()),
    ('response', StringType()),
    ('agent', StringType())
]

logfields = [StructField(*field) for field in LOGS_SCHEMA]
schema_logs = StructType(logfields)

ECOMMERCE_LOGS_PATH="s3a://" + args['ECOMMERCE_LOGS_BUCKET'] + "/" + "ecommerce_logs/"
df_ecommerce_logs = spark.read.json(ECOMMERCE_LOGS_PATH, schema=schema_logs)
df_ecommerce_logs = df_ecommerce_logs.withColumn('ip_number',ip_to_country_udf('remote_ip'))
df_ecommerce_logs = df_ecommerce_logs.withColumn("ip_number_int", df_ecommerce_logs['ip_number'].cast('int')).drop('ip_number').withColumnRenamed('ip_number_int', 'ip_number')
df_ecommerce_logs.registerTempTable('ecommerce')

df_ecommerce_country = spark.sql("SELECT e.time, e.remote_ip, i.country_name, e.ip_number, e.request, e.response, e.agent " \
                                 " FROM ecommerce e JOIN iplocation i WHERE ip1 <= ip_number AND ip2 >= ip_number")

ECOMM_LOGS_SILVER_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['SILVER_LAYER_NAMESPACE'] + "/" + "ecommerce_country"

try:   
    deltaTable = DeltaTable.forPath(spark, ECOMM_LOGS_SILVER_TABLE_PATH)
    df_ecommerce_country.printSchema()
    if deltaTable:
        deltaTable.alias("logs").merge(
             df_ecommerce_country.alias("logs_incr"),
             "logs.remote_ip = logs_incr.remote_ip") \
            .whenNotMatchedInsertAll() \
            .execute()
        
except:
    print("Delta table does not exist")
    df_ecommerce_country.write.format("delta").save(ECOMM_LOGS_SILVER_TABLE_PATH)
    deltaTable = DeltaTable.forPath(spark, ECOMM_LOGS_SILVER_TABLE_PATH)
    deltaTable.generate("symlink_format_manifest")
# Function to read ecommerce transactions from bronze layer and merge data to delta table

BRONZE_ECOMMERCE_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['BRONZE_LAYER_ECOMMERCE_NAMESPACE'] + "/" + args['ECOMMERCE_STREAM_DATE']
SILVER_ECOMMERCE_TABLE_PATH="s3a://" + args['S3_BUCKET'] + "/" + args['SILVER_LAYER_NAMESPACE'] + "/" + "ecommerce_orders"

df_ecommerce_transactions = spark.read.parquet(BRONZE_ECOMMERCE_TABLE_PATH)
df_ecommerce_transactions=flatten_df(df_ecommerce_transactions)
df_ecommerce_transactions=df_ecommerce_transactions.drop('id', 'eventtype', 'subject', 'eventtime', 'dataversion') \
                                                   .withColumnRenamed('data_customer_name', 'customer_name') \
                                                   .withColumnRenamed('data_address', 'address') \
                                                   .withColumnRenamed('data_city', 'city') \
                                                   .withColumnRenamed('data_postalcode', 'postalcode') \
                                                   .withColumnRenamed('data_country', 'country') \
                                                   .withColumnRenamed('data_phone', 'phone') \
                                                   .withColumnRenamed('data_email', 'email') \
                                                   .withColumnRenamed('data_product_name', 'product_name') \
                                                   .withColumnRenamed('data_order_date', 'order_date') \
                                                   .withColumnRenamed('data_currency', 'currency') \
                                                   .withColumnRenamed('data_order_mode', 'order_mode') \
                                                   .withColumnRenamed('data_sale_price', 'sale_price') \
                                                   .withColumnRenamed('data_order_number', 'order_number') 

df_ecommerce_transactions=curate_columns(df_ecommerce_transactions, 'E')
df_ecommerce_transactions=df_ecommerce_transactions.withColumn("order_number",col("order_number").cast(IntegerType())).withColumn("sale_price",col("sale_price").cast(FloatType()))
df_ecommerce_transactions.show(3)
df_ecommerce_transactions.printSchema()

try:   
    deltaTable = DeltaTable.forPath(spark, SILVER_ECOMMERCE_TABLE_PATH)
    if deltaTable:
        print("Delta table exists")
        DELTA_TABLE_ALIAS="ecomm_delta_table"
        INCREMENTAL_TABLE_ALIAS="ecomm_delta_table_incremental"
        JOIN_CONDITION=DELTA_TABLE_ALIAS + "." + "email" + "=" + INCREMENTAL_TABLE_ALIAS + "." + "email"
        deltaTable.alias(DELTA_TABLE_ALIAS).merge(
                    source=df_ecommerce_transactions.alias(INCREMENTAL_TABLE_ALIAS),
                    condition=fn.expr(JOIN_CONDITION)) \
                    .whenMatchedUpdateAll()            \
                    .whenNotMatchedInsertAll()         \
                    .execute()
except:
    df_ecommerce_transactions.write.format("delta").save(SILVER_ECOMMERCE_TABLE_PATH)
    deltaTable = DeltaTable.forPath(spark, SILVER_ECOMMERCE_TABLE_PATH)
    deltaTable.generate("symlink_format_manifest")
# Create a curation schema in glue catalog

spark.sql("CREATE DATABASE IF NOT EXISTS " + args['GLUE_SILVER_DATABASE'])
# Create tables in glue catalog

create_glue_table("store_customers", args['S3_BUCKET'], args['SILVER_LAYER_NAMESPACE'], args['GLUE_SILVER_DATABASE'], "customer_id integer, customer_name string, updated_at timestamp ,email string, country string, phone string, city string, postalcode string, credit_card string, address string")
create_glue_table("products", args['S3_BUCKET'], args['SILVER_LAYER_NAMESPACE'], args['GLUE_SILVER_DATABASE'], "product_code integer, product_name string, product_category string, updated_at timestamp")
create_glue_table("store_orders", args['S3_BUCKET'], args['SILVER_LAYER_NAMESPACE'], args['GLUE_SILVER_DATABASE'], "order_number integer, customer_id integer, product_id integer, order_date date, units integer, sale_price double, currency string, order_mode string, updated_at timestamp, sale_price_usd float")
create_glue_table("ecommerce_country", args['S3_BUCKET'], args['SILVER_LAYER_NAMESPACE'], args['GLUE_SILVER_DATABASE'], "time string, remote_ip string, country_name string, ip_number integer, request string, response string, agent string")
create_glue_table("ecommerce_orders", args['S3_BUCKET'], args['SILVER_LAYER_NAMESPACE'], args['GLUE_SILVER_DATABASE'], "customer_name string, product_name string, order_date date, currency string, order_mode string, sale_price float, order_number integer, sale_price_usd float, email string, country string, phone string, city string, postalcode string,address string")
job.commit()