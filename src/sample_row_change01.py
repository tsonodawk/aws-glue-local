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
## @args: [database = "glue-sample-db", table_name = "sns_visit_count_data_by_hour", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue-sample-db", table_name = "sns_visit_count_data_by_hour", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("visit_date", "string", "visit_date", "string"), ("week_name", "string", "week_name", "string"), ("visit_hour", "string", "visit_hour", "string"), ("visit_count", "long", "visit_count", "long"), ("store_code", "string", "store_code", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("visit_date", "string", "visit_date", "string"), ("week_name", "string", "week_name", "string"), ("visit_hour", "string", "visit_hour", "string"), ("visit_count", "long", "visit_count", "long"), ("store_code", "string", "store_code", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://tyd-glue-data/sample-data/output/sns_visit_count/test01"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://tyd-glue-data/sample-data/output/sns_visit_count/test01"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
