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
journal_schema = StructType(journal_fields())
journal_dir = 's3://journal-filter-data/'
sdf_journal_filter_data = spark.read.csv(journal_dir, header=True, encoding='utf-8', schema=journal_schema)
# sdf_journal_filter_data.show()

item_schema = StructType(item_fields())
item_dir = 's3://mekiki-data-bucket/mekiki-data/input-output/item-master/'
sdf_item_master = spark.read.csv(item_dir, header=False, encoding='utf-8', schema=item_schema)
# sdf_item_master.show()

sales_start_schema = StructType(stales_start_day_fields())
sales_start_dir = 's3://sales-start-day/'
sdf_sales_start_day = spark.read.csv(sales_start_dir, header=True, encoding='utf-8', schema=sales_start_schema)


print("=====================================")
print("exec precess")
print("=====================================")
# ==============================
# 販売がある商品コード一覧を作成
# create a list of itmcd with sales
# ==============================
# 終了日をセット(pysparkのfunctionを使用)
# 基準日に月を加算し、対象付きの最終日付をセット
sdf_parameter_add = sdf_parameter.select(
    func.add_months(sdf_parameter.reference_date, int(param_month_later)).alias('end_date'),
)

# sdf_parameter_add.show()

start_date = param_reference_date
# print(start_date)
end_date = sdf_parameter_add.first()['end_date']
# print(end_date)

# ==============================
# 指定期間で、販売がある店舗・商品コード一覧を作成
# create a list of itmcd with sales
# ==============================
sdf_sales_shop_itmcd = sdf_journal_filter_data.filter(
    (sdf_journal_filter_data.ymd >= start_date)
    & (sdf_journal_filter_data.ymd < end_date)
    & (sdf_journal_filter_data.qty > 0)
).select('shop', 'itmcd').distinct()

# 出力
out_gdf_sales_itmcd_start_ymd = s3_write_spark_dataframe_single_file(
    sdf_sales_shop_itmcd,
    's3://sales-shop-itmcd'
    )

# ==============================
# 指定期間以降で販売された（だろう）商品と販売開始日
# =============================
# 開始日・終了日をセット(pysparkのfunctionを使用)
sdf_parameter_add = sdf_parameter.select(
    func.add_months(sdf_parameter.reference_date, -int(param_month_ago)).alias('sales_start_date'),
)

sales_start_date = sdf_parameter_add.first()['sales_start_date']
# print(sales_start_date)

sdf_sales_start_day_filter = sdf_sales_start_day.filter(
    sdf_sales_start_day.sales_start_day >= sales_start_date
)

# 販売がある商品に、新商品（想定）の商品で絞り込んで販売開始日を付与する
sdf_new_release_items = sdf_sales_shop_itmcd.join(
    sdf_sales_start_day_filter,
    [
        sdf_sales_shop_itmcd.shop == sdf_sales_start_day_filter.shop,
        sdf_sales_shop_itmcd.itmcd == sdf_sales_start_day_filter.itmcd
    ],
    'inner'
).select(
    sdf_sales_shop_itmcd['*'],
    sdf_sales_start_day_filter.sales_start_day
)

# sdf_new_release_items.show()

# 商品マスタの情報を付与する
sdf_new_release_items_join_master = sdf_new_release_items.join(
    sdf_item_master,
    sdf_new_release_items.itmcd == func.lpad(sdf_item_master.itmcd, 18, '0'),
    "inner"
).select(
    sdf_new_release_items.shop,
    sdf_item_master.cate1,
    sdf_item_master.cate2,
    sdf_item_master.cate3,
    sdf_item_master.cate4,
    sdf_item_master.cate5,
    sdf_new_release_items.itmcd,
    sdf_item_master.itmnm,
    sdf_new_release_items.sales_start_day,
)

# 出力
out_gdf_sales_itmcd_start_ymd = s3_write_spark_dataframe_single_file(
    sdf_new_release_items_join_master,
    's3://new-release-items'
    )

print("=====================================")
print("job commit")
print("=====================================")
job.commit()
