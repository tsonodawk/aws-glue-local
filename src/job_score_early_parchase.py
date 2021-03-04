import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def s3_write_spark_dataframe_single_file(spark_data_frame, s3_path):
    """ S3にファイルを出力する
    args:
        spark_data_frame: spark形式のDataFrame
        s3_path: 出力先S3のフルパス
    return:
    """
    # 複数ファイル出力を一つにする
    sdf_single = spark_data_frame.coalesce(1)
    # PySparkのDataFrameをGlueのDataFrameに変換
    gdf_single = DynamicFrame.fromDF(sdf_single, glueContext, 'gdf_single')

    # S3に出力
    s3_write_dynamic_frame = glueContext.write_dynamic_frame.from_options(
        frame=gdf_single,
        connection_type='s3',
        connection_options={
            'path': s3_path
        },
        format='csv',
        transformation_ctx = "s3_write_dynamic_frame ",
    )
    return s3_write_dynamic_frame

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

# MEMO: 取り込みpath確認
sdf_journal_filter_data = spark.read.csv(
    's3://journal-filter-data',
    header=True, encoding='utf-8', schema=journal_schema
    )


# MEMO: 取り込みpath確認
sdf_sales_itmcd_start_data  = spark.read.csv(
    's3://sales-itmcd-start-ymd',
    header=True, encoding='utf-8'
    )



print("=====================================")
print("Start S3 DataRead")
print("=====================================")
# 顧客IDが含まれるデータの抽出
# ますはcstidを使用
sdf_sales_filter_in_customerid = sdf_journal_filter_data.filter(
    (sdf_journal_filter_data.cstid > 0)
    & (sdf_journal_filter_data.Ymd >= "2020-05-23")
    )

