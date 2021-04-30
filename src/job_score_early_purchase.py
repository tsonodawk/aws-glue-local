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


def popular_items_fields():
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


def score_master_early_purchase_fields():
    fields_list = [
        StructField("days_later_buy", IntegerType(), True),
        StructField("early_purchase_score", IntegerType(), True),
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
# journal_filter読み込み
journal_schema = StructType(journal_fields())
journal_dir = 's3://journal-filter-data/'
sdf_journal_filter_data = spark.read.csv(journal_dir, header=True, encoding='utf-8', schema=journal_schema)

# 新商品リストを読み込み
popular_items_schema = StructType(popular_items_fields())
pupular_items_dir = 's3://new-release-items/'
sdf_popular_items = spark.read.csv(pupular_items_dir, header=True, encoding='utf-8', schema=popular_items_schema)

# sdf_popular_items.show()

# # 早期購入スコア
# score_master_early_purchase_schema = StructType(score_master_early_purchase_fields())
# master_early_purchase_dir = 's3://mekiki-data-bucket/mekiki-data/input-output/score-master-early-purchase/'
# sdf_score_master_early_purchase = spark.read.csv(master_early_purchase_dir, header=True, encoding='utf-8', schema=score_master_early_purchase_schema )

# sdf_score_master_early_purchase.show()

print("=====================================")
print("exec precess")
print("=====================================")
# 遡り日数の取得
# 開始日・終了日をセット(pysparkのfunctionを使用)
sdf_parameter_add = sdf_parameter.select(
    func.add_months(sdf_parameter.reference_date, -int(param_month_ago)).alias('sales_start_date'),
)

sales_start_date = sdf_parameter_add.first()['sales_start_date']
# print(sales_start_date)

# 指定期間以上の日付で、顧客IDが含まれるデータの抽出
sdf_sales_filter_in_customerid = sdf_journal_filter_data.filter(
    (sdf_journal_filter_data.cadid != "000000000000000000")
    & (sdf_journal_filter_data.ymd >= sales_start_date)
    )
# sdf_sales_filter_in_customerid .show()

# 顧客IDが含まれるデータで、対象新商品のみ抽出し、店舗・商品・顧客毎に最小日付（最初に買った日）を出力する
sdf_sales_customer_buy_min_ymd = sdf_sales_filter_in_customerid.alias('df_cus').join(
    sdf_popular_items.alias('df_pop'),
    [
        # target shop and itmcd
        col('df_cus.shop') == col('df_pop.shop'),
        col('df_cus.itmcd') == col('df_pop.itmcd')
    ],
    'inner'
).groupBy(
    'df_cus.shop', 'df_cus.itmcd', 'df_cus.cadid', 'df_pop.sales_start_day'
).agg(
    func.min('df_cus.ymd').alias('buy_min_ymd')
)

# sdf_sales_customer_buy_min_ymd.show()

# 販売開始日との日数差を出す
sdf_days_later_buy = sdf_sales_customer_buy_min_ymd.alias('df1').withColumn(
    'days_later_buy',
    func.datediff(col('df1.buy_min_ymd'), col('df1.sales_start_day'))
    # func.datediff(col('min_ymd'), col('buy_min_ymd'))
    # (col('min_ymd') - col('buy_min_ymd')).days
    ).orderBy(
        'days_later_buy'
        )

# sdf_days_later_buy.show()

# スコアの計算(1/n で逆算値を使用する)
sdf_score_early_purchase = sdf_days_later_buy.withColumn(
    'early_purchase_score', 1 / (sdf_days_later_buy.days_later_buy + 1)
)

# sdf_score_early_purchase.show()

# 出力
out_gdf_score_early_purchase = s3_write_spark_dataframe_single_file(
    sdf_score_early_purchase,
    's3://score-early-purchase'
    )

print("=====================================")
print("job commit")
print("=====================================")
job.commit()

