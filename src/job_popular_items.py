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
from pyspark.sql.types import LongType, DoubleType
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

def stales_start_day_fields():
    fields_list = [
        StructField("shop", StringType(), True),
        StructField("itmcd", StringType(), True),
        StructField("sales_start_day", DateType(), True),
    ]

    return fields_list

def new_release_items_fields():
    fields_list = [
        StructField("shop", StringType(), True),
        StructField("cate1", StringType(), True),
        StructField("cate2", StringType(), True),
        StructField("cate3", StringType(), True),
        StructField("cate4", StringType(), True),
        StructField("cate5", StringType(), True),
        StructField("itmcd", StringType(), True),
        StructField("itmnm", StringType(), True),
        StructField("sales_start_day", DateType(), True),
    ]

    return fields_list

def popular_amount_abc_fields():
    fields_list = [
        StructField("shop", StringType(), True),
        StructField("cate1", StringType(), True),
        StructField("cate2", StringType(), True),
        StructField("cate3", StringType(), True),
        StructField("itmcd", StringType(), True),
        StructField("item_sum_amount", LongType(), True),
        StructField("shop_cate_amount_all", LongType(), True),
        StructField("ratio", DoubleType(), True),
        StructField("cumsum_ratio", DoubleType(), True),
        StructField("abc_rank", StringType(), True),
    ]

    return fields_list


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

param_abc_s = sdf_parameter.first()['abc_s']
param_abc_a = sdf_parameter.first()['abc_a']
param_abc_b = sdf_parameter.first()['abc_b']
param_abc_c = sdf_parameter.first()['abc_c']

# ==============================
# データファイル読み込み
# ==============================
# popular_amount_abc
amount_abc_schema = StructType(popular_amount_abc_fields())
amount_abc_dir = 's3://popular-amount-abc/'
sdf_popular_amount_abc = spark.read.csv(amount_abc_dir, header=True, encoding='utf-8', schema=amount_abc_schema)


# 新商品リストを読み込み
new_release_schema = StructType(new_release_items_fields())
new_release_dir = 's3://new-release-items/'
sdf_new_release_items = spark.read.csv(new_release_dir, header=True, encoding='utf-8', schema=new_release_schema)

# item_master
item_schema = StructType(item_fields())
item_dir = 's3://mekiki-data-bucket/mekiki-data/input-output/item-master/'
sdf_item_master = spark.read.csv(item_dir, header=False, encoding='utf-8', schema=item_schema)
# sdf_item_master.show()

print("=====================================")
print("exec precess")
print("=====================================")
# ==============================
# journalの期間を、販売開始想定期間以降に絞り込み
# =============================
# Sランク商品のみ
sdf_amount_rank_a = sdf_popular_amount_abc.filter(sdf_popular_amount_abc.abc_rank == 'S')
# sdf_amount_rank_a.show(100)

# 新商品リストからAランク商品のみに絞り込む
sdf_popular_items = sdf_new_release_items_rank_a = sdf_new_release_items.join(
    sdf_amount_rank_a,
    [
        sdf_new_release_items.shop == sdf_amount_rank_a.shop,
        sdf_new_release_items.itmcd == sdf_amount_rank_a.itmcd
    ],
    'inner'
).select(sdf_new_release_items['*'])

# sdf_popular_items.show()

# 出力
out_gdf_popular_items = s3_write_spark_dataframe_single_file(
    sdf_popular_items,
    's3://popular-items'
    )

print("=====================================")
print("job commit")
print("=====================================")
job.commit()

