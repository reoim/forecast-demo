import sys
import boto3
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

session = boto3.Session(region_name='ap-northeast-2') 
glue_client = session.client(service_name='glue')
s3Bucket = "s3://forecast-demogo-bucket"
s3Folder = "/forecast_data"

# Set source data with raw_data in S3
datasource = glueContext.create_dynamic_frame.from_catalog(database = "forecast_raw_db", table_name = "raw_data")

df1 = datasource.toDF()

# RenameField.apply(frame = df, old_name = "sales_quantity", new_name = "target_value")
df2 = df1.withColumnRenamed("sales_quantity","target_value")
data_frame=DynamicFrame.fromDF(df2, glueContext, "data_frame")

glueContext.write_dynamic_frame.from_options(frame = data_frame, connection_type = "s3", connection_options = {"path":s3Bucket+s3Folder}, format = "csv")
