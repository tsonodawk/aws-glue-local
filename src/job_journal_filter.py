import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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
#######
# 本番ではGlueテーブルから読み込む
# ↓
# .toDF() でSparkDataFrameへ変換を想定(データ型はGlueのテーブル型になってると思う)
#
# GlueテーブルからDataFrameの作成例：
# DataSource0 = glueContext.create_dynamic_frame.from_catalog(
#     database = "glue-sample-db",
#     table_name = "sns_sales_receipt_data",
#     transformation_ctx = "DataSource0"
#     )
#
#######


# SparkDataFrame形式でS3からファイルを読み込んでしまう

journal_schema = StructType([
    StructField("cpcd2", IntegerType(), True),
    StructField("Shop", StringType(), True),
    StructField("Ymd", DateType(), True),
    StructField("Hm", StringType(), True),
    StructField("Reg", StringType(), True),
    StructField("Num", StringType(), True),
    StructField("Seq", StringType(), True),
    StructField("dtype", StringType(), True),
    StructField("itmcd", StringType(), True),
    StructField("cate1", StringType(), True),
    StructField("cate2", StringType(), True),
    StructField("cate3", StringType(), True),
    StructField("cate4", StringType(), True),
    StructField("cate5", StringType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("Amt", IntegerType(), True),
    StructField("Prf", IntegerType(), True),
    StructField("Type", StringType(), True),
    StructField("Pay", StringType(), True),
    StructField("cadid", StringType(), True),
    StructField("cid", StringType(), True),
    StructField("cstid", StringType(), True),
    StructField("itmid", StringType(), True),
    StructField("bgnno", StringType(), True),
    StructField("bgntp", StringType(), True),
    StructField("bgnid", StringType(), True),
    StructField("itmcd_org", StringType(), True),
    StructField("opt01", StringType(), True),
    StructField("opt02", StringType(), True),
    StructField("opt03", StringType(), True),
    StructField("opt04", StringType(), True),
    StructField("opt05", StringType(), True),
    StructField("num_org", StringType(), True),
])

sdf_journal_all = spark.read.csv(
    "s3://mekiki-data-bucket/mekiki-data/input-output/journal-data",
    header=False,
    encoding='utf-8',
    schema=journal_schema)

# sdf_journal_all.show()

print("=====================================")
print("filter category")
print("=====================================")
# 本来は cate1 = 11 and cate2 = 3108 and cate3 = 3357 
# TODO: categoryを引数 or パラメータ化の検討

sdf_journal_filter = sdf_journal_all.filter(
    (sdf_journal_all.cate1 == "000011")
    & (sdf_journal_all.cate2 == "000000")
    & (sdf_journal_all.cate3 == "000000")
    )


print("=====================================")
print("Start Write S3")
print("=====================================")
# 複数ファイル出力を一つにする場合
sdf_journal_filter = sdf_journal_filter.coalesce(1)

# sdf_journal_filter.write.mode('overwrite').csv(
#     "s3://journal-filter-data"
#     )

# sdf_journal_filter.write.mode('overwrite').csv(
#     "s3://mekiki-data-bucket/mekiki-data/input-output/journarl-filter-data",
#     header=True
#     )
# sdf_journal_filter.show()

# sdf_journal_filter.write.mode('append').csv(
#     "s3://mekiki-data-bucket/"
# )

# sdf_journal_filter.write.mode('overwrite').parquet(
#     "s3://mekiki-data-bucket/mekiki-data/input-output/journarl-filter-data/"
#     )

# PySparkのDataFrameをGlueのDataFrameに変換
gdf_journal_filter = DynamicFrame.fromDF(sdf_journal_filter, glueContext, 'sdf_journal_filter')
# gdf_journal_filter.show()

# s3出力（なぜかバケット直下しかうまくいかない。なぜだ・・・）
# 本番では他ディレクトリ配下に作成可能
out_gdf_journal_filter = glueContext.write_dynamic_frame.from_options(
    frame=gdf_journal_filter,
    connection_type='s3',
    connection_options={
        'path': 's3://journal-filter-data'
    },
    format='csv',
    transformation_ctx = "out_gdf_journal_filter ",
)

print("=====================================")
print("job commit")
print("=====================================")
job.commit()
