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

def journal_fields():
    fields_list = [
        StructField("cpcd2", IntegerType(), True),
        StructField("shop", StringType(), True),
        StructField("ymd", DateType(), True),
        StructField("hm", StringType(), True),
        StructField("reg", StringType(), True),
        StructField("num", StringType(), True),
        StructField("seq", StringType(), True),
        StructField("dtype", StringType(), True),
        StructField("itmcd", StringType(), True),
        StructField("cate1", StringType(), True),
        StructField("cate2", StringType(), True),
        StructField("cate3", StringType(), True),
        StructField("cate4", StringType(), True),
        StructField("cate5", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("amt", IntegerType(), True),
        StructField("prf", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("pay", StringType(), True),
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
        ]
    return fields_list

def item_fields():
    fields_list = [
        StructField("itmid", StringType(), True),
        StructField("cpcd1", StringType(), True),
        StructField("itmcd", StringType(), True),
        StructField("jancd", StringType(), True),
        StructField("itmnm", StringType(), True),
        StructField("cate1", StringType(), True),
        StructField("cate2", StringType(), True),
        StructField("cate3", StringType(), True),
        StructField("cate4", StringType(), True),
        StructField("cate5", StringType(), True),
        StructField("makcd", StringType(), True),
        StructField("costp", StringType(), True),
        StructField("sellp", StringType(), True),
        StructField("mrkup", StringType(), True),
        StructField("spcd1", StringType(), True),
        StructField("spcd2", StringType(), True),
        StructField("deldv", StringType(), True),
        StructField("adddt", StringType(), True),
        StructField("edtdt", StringType(), True),
        StructField("deldt", StringType(), True),
    ]

    return fields_list

# SparkDataFrame形式でS3からファイルを読み込んでしまう

# ==============================
# parameterファイル読み込み
# ==============================
file_path = 's3://mekiki-data-bucket/mekiki-data/input-output/parameter-master/'
sdf_parameter = spark.read.csv(file_path, header=True)

param_cate1 = sdf_parameter.first()['cate1']
param_cate2 = sdf_parameter.first()['cate2']
param_cate3 = sdf_parameter.first()['cate3']
param_reference_date = sdf_parameter.first()['reference_date']
param_month_ago = sdf_parameter.first()['month_ago']
param_month_later = sdf_parameter.first()['month_later']

# ==============================
# データファイル読み込み
# ==============================
# journalファイル読み込み
journal_schema = StructType(journal_fields())
journal_dir = "s3://mekiki-data-bucket/mekiki-data/input-output/journal-data"
sdf_journal_data = spark.read.csv(journal_dir, header=False, encoding='utf-8', schema=journal_schema)

# item読み込み
item_schema = StructType(item_fields())
item_dir = 's3://mekiki-data-bucket/mekiki-data/input-output/item-master/'
sdf_item_master = spark.read.csv(item_dir, header=False, encoding='utf-8', schema=item_schema)

print("=====================================")
print("filter category")
print("=====================================")
# journalにitem_masterをjoin
# 特定フィルターにしぼる
sdf_journal_filter_data = sdf_journal_data.join(
    sdf_item_master,
    sdf_journal_data.itmcd == func.lpad(sdf_item_master.itmcd, 18, '0'),
    'inner'
).select(
    sdf_journal_data.cpcd2,
    sdf_journal_data.shop,
    sdf_journal_data.ymd,
    sdf_journal_data.hm,
    sdf_journal_data.reg,
    sdf_journal_data.num,
    sdf_journal_data.seq,
    sdf_journal_data.dtype,
    sdf_journal_data.itmcd,
    # >> item cate
    sdf_item_master.cate1,
    sdf_item_master.cate2,
    sdf_item_master.cate3,
    sdf_item_master.cate4,
    sdf_item_master.cate5,
    # << item cate
    sdf_journal_data.qty,
    sdf_journal_data.amt,
    sdf_journal_data.prf,
    sdf_journal_data.type,
    sdf_journal_data.pay,
    sdf_journal_data.cadid,
    sdf_journal_data.cid,
    sdf_journal_data.cstid,
    sdf_journal_data.itmid,
    sdf_journal_data.bgnno,
    sdf_journal_data.bgntp,
    sdf_journal_data.bgnid,
    sdf_journal_data.itmcd_org,
    sdf_journal_data.opt01,
    sdf_journal_data.opt02,
    sdf_journal_data.opt03,
    sdf_journal_data.opt04,
    sdf_journal_data.opt05,
    sdf_journal_data.num_org,
).filter(
    (sdf_item_master.cate1 == param_cate1)
    & (sdf_item_master.cate2 == param_cate2)
    & (sdf_item_master.cate3 == param_cate3)
)

print("=====================================")
print("Start Write S3")
print("=====================================")

# 複数ファイル出力を一つにする場合
sdf_journal_filter_data = sdf_journal_filter_data.coalesce(1)

# sdf_journal_filter_data.show()

# PySparkのDataFrameをGlueのDataFrameに変換
gdf_write_data = DynamicFrame.fromDF(sdf_journal_filter_data, glueContext, 'gdf_journal_filter_data')

# s3出力（なぜかバケット直下しかうまくいかない。なぜだ・・・）
# 本番では他ディレクトリ配下に作成可能
out_gdf = glueContext.write_dynamic_frame.from_options(
    frame=gdf_write_data,
    connection_type='s3',
    connection_options={
        'path': 's3://journal-filter-data'
    },
    format='csv',
    transformation_ctx = "out_gdf",
)

print("=====================================")
print("job commit")
print("=====================================")
job.commit()
