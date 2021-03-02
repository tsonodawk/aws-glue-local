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

gds_visit_count_data_by_hour = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {'paths': ["s3://test-bucket/sns-visit_count_data_by_hour/"]},
    format = "csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx = "gds_visit_count_data_by_hour"
)


# print("=====================================")
# print("show gds")
# print("=====================================")
# gds_store_master.show()
# gds_visit_count_data_by_hour.show()

# Glue DynamicFrameでの列操作
# gds_store_master.show()
# gds_store_master.select_fields(["store_code"]).show()

print("=====================================")
print("show debug")
print("=====================================")
df_store_master = gds_store_master.toDF()
df_visit_count_data_by_hour = gds_visit_count_data_by_hour.toDF()

# df_store_master.show()
# df_visit_count_data_by_hour.show()

print("=====================================")
print("show join by pyspark")
print("=====================================")
df_join = df_visit_count_data_by_hour.join(df_store_master, df_visit_count_data_by_hour.store_code == df_store_master.store_code, 'inner')

# df_join.show()


print("=====================================")
print("columun rename and show join by pyspark")
print("=====================================")
# store_master側の列名を変更(後で消すため)
df_store_master02 = df_store_master.withColumnRenamed('store_code', 'master_store_code')
# df_store_master.show()

# join
df_join = df_visit_count_data_by_hour.join(
    df_store_master02,
    df_visit_count_data_by_hour.store_code == df_store_master02.master_store_code,
    'inner'
    )

# 列削除
df_join_drop = df_join.drop('master_store_code')

# df_join_drop.show()

# 並び替えて表示
# df_join_drop.select(["store_code", "store_name", "visit_date", "week_name", "visit_hour", "visit_count"]).show()

# 並び替えとフィルター
# df_join_drop.select(["store_code", "store_name", "visit_date", "week_name", "visit_hour", "visit_count"]).filter(df_join_drop.visit_date == '2019-05-08').show()

print("=====================================")
print("sum")
print("=====================================")
# 型変換
df_numeric = df_join_drop.withColumn("visit_count", df_join_drop.visit_count.cast('int'))

df_numeric.groupBy("visit_date").sum('visit_count').show()
# df_numeric.groupBy("visit_date").sum().show()
df_numeric.select(["visit_date", "visit_count"]).groupby("visit_date").sum().show()

# df_join_drop.groupBy("visit_date").agg(sum('visit_count')).show()
# df_join_drop.select(["visit_date", "visit_count"]).groupBy("visit_date").agg(sum()).show()


print("=====================================")
print("test moromoro")
print("=====================================")

# df_join_drop.count()

# df_join_drop.select(
#     ["store_code", "store_name", "visit_date", "week_name", "visit_hour", "visit_count"]
#     ).summary().show()

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

