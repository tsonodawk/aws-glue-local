import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"withHeader":True,"separator":",","quoteChar":"\""}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://tyd-glue-data/sample-data/input/sns-store_master/"]}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(
    format_options = {"withHeader":True,"separator":",","quoteChar":"\""},
    connection_type = "s3",
    format = "csv",
    connection_options = {"paths": ["s3://test-bucket/sns-store_master/"]},
    transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [format_options = {"withHeader":True,"separator":",","quoteChar":"\""}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://tyd-glue-data/sample-data/input/sns-visit_count_data_by_hour/"]}, transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
# DataSource1 = glueContext.create_dynamic_frame.from_options(format_options = {"withHeader":True,"separator":",","quoteChar":"\""}, connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://tyd-glue-data/sample-data/input/sns-visit_count_data_by_hour/"]}, transformation_ctx = "DataSource1")

DataSource0.toDF().show()
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://tyd-glue-data/sample-data/output/YYZ-Tickets-Job/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform1]
DataSink0 = glueContext.write_dynamic_frame.from_options(
    frame = DataSource0,
    connection_type = "s3",
    format = "csv",
    connection_options = {"path": "s3://dcom-bucket/", "partitionKeys": []},
    transformation_ctx = "DataSink0")

job.commit()

