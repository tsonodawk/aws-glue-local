import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# import numpy as np
# import pandas as pd


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


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

print("=====================================")
print("データフレームの変換")
print("=====================================")

# Glue -> Spark
df_store_master = gds_store_master.toDF()

# Spark -> pandas
pd_df = df_store_master.select("*").toPandas()
# pandaで列の並び替え
pd_df_col = pd_df.reindex(columns=['store_name', 'store_code'])

# pandas -> spark
ps_df = spark.createDataFrame(pd_df_col)

ps_df.show()

# spark -> glue
gdf_df = DynamicFrame.fromDF(ps_df, glueContext, 'gdf_df')
gdf_df.show()


job.commit()

