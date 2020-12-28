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



# sink = glueContext.getSink(
#     connection_type="s3",
#     path="s3://test-bucket",
#     enableUpdateCatalog=True,
#     updateBehavior="UPDATE_IN_DATABASE",
#     partitionKeys=["partition_key0", "partition_key1"])
# sink.setFormat("<format>")
# sink.setCatalogInfo(catalogDatabase="test-bucket-db", catalogTableName="store_master")
# # sink.writeFrame(last_transform)


print("=====================================")
print("Start S3 DataRead")
print("=====================================")
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue-sample-db", table_name = "sns_visit_count_data_by_hour", transformation_ctx = "datasource0")
# datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "test-bucket-db", table_name = "store_master", transformation_ctx = "datasource0")
# gds: glue-datastore
gds_store_master = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {'paths': ["s3://test-bucket/sns-store_master"]},
    format = "csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx = "datasource0"
)

# gds: glue-datastore
# gds_visit_count_data_by_hour = glueContext.create_dynamic_frame.from_options(
#     connection_type = "s3",
#     connection_options = {'paths': ["s3://test-bucket/visit_count_data_by_hour/"]},
#     format = "csv",
#     format_options={"withHeader": True, "separator": ","},
#     transformation_ctx = "gds_visit_count_data_by_hour"
# )


print("=====================================")
print("show debug")
print("=====================================")
# datasource0.toDF().show()
gds_store_master.toDF().show()

# gds_visit_count_data_by_hour.toDF().show()

# ## @type: ApplyMapping
# ## @args: [mapping = [("visit_date", "string", "visit_date", "string"), ("week_name", "string", "week_name", "string"), ("visit_hour", "string", "visit_hour", "string"), ("visit_count", "long", "visit_count", "long"), ("store_code", "string", "store_code", "string")], transformation_ctx = "applymapping1"]
# ## @return: applymapping1
# ## @inputs: [frame = datasource0]
# applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("visit_date", "string", "visit_date", "string"), ("week_name", "string", "week_name", "string"), ("visit_hour", "string", "visit_hour", "string"), ("visit_count", "long", "visit_count", "long"), ("store_code", "string", "store_code", "string")], transformation_ctx = "applymapping1")
# ## @type: DataSink
# ## @args: [connection_type = "s3", connection_options = {"path": "s3://tyd-glue-data/sample-data/output/sns_visit_count/test01"}, format = "csv", transformation_ctx = "datasink2"]
# ## @return: datasink2
# ## @inputs: [frame = applymapping1]
# datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://tyd-glue-data/sample-data/output/sns_visit_count/test01"}, format = "csv", transformation_ctx = "datasink2")


job.commit()

