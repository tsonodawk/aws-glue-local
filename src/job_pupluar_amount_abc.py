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
# journal
journal_schema = StructType(journal_fields())
journal_dir = 's3://journal-filter-data/'
sdf_journal_filter_data = spark.read.csv(journal_dir, header=True, encoding='utf-8', schema=journal_schema)
# sdf_journal_filter_data.show()

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
# 開始日・終了日をセット(pysparkのfunctionを使用)
sdf_parameter_add = sdf_parameter.select(
    func.add_months(sdf_parameter.reference_date, -int(param_month_ago)).alias('sales_start_date'),
)
sales_start_date = sdf_parameter_add.first()['sales_start_date']
# print(sales_start_date)

sdf_journal_filter_date_add = sdf_journal_filter_data.filter(
    sdf_journal_filter_data.ymd >= sales_start_date
)
# sdf_journal_filter_date_add.show()


# ==============================
# 店舗・商品別販売金額のABC分析
# ==============================
# 店舗・分類の全合計数
sdf_shop_cate_amount_all = sdf_journal_filter_date_add.groupBy(
    'shop','cate1','cate2','cate3'
).agg(
    func.sum('amt').alias('shop_cate_amount_all')
    )

# sdf_sales_amount_all.show()

# 商品別販売数の合計
sdf_item_sum_amount = sdf_journal_filter_date_add.groupBy(
            'shop', 'cate1', 'cate2', 'cate3', 'itmcd'
        ).agg(
            func.sum('amt').alias('item_sum_amount')
        ).sort(
            func.desc('item_sum_amount')
        )

# sdf_item_sum_amount.show()

# 構成比作成
# df_sum_amount_ratio = df_sum_amount.withColumn(
#     'ratio', df_sum_amount.sum_amount / amount_all * 100)
# df_sum_amount_ratio.show()
sdf_sum_amount_ratio = sdf_item_sum_amount.alias('df1').join(
    sdf_shop_cate_amount_all.alias('df2'),
    [
        col('df1.shop') == col('df2.shop'),
        col('df1.cate1') == col('df2.cate1'),
        col('df1.cate2') == col('df2.cate2'),
        col('df1.cate3') == col('df2.cate3')
    ],
    'inner'
).select(
    'df1.*',
    'df2.shop_cate_amount_all'
).withColumn(
    'ratio', col('df1.item_sum_amount') / col('df2.shop_cate_amount_all') * 100
)

# sdf_sum_amount_ratio.show()

# 構成比の累計作成
sdf_cumsum_ratio = sdf_sum_amount_ratio.withColumn(
    'cumsum_ratio',
    func.sum(
        sdf_sum_amount_ratio.ratio
        ).over(
            Window.partitionBy(
                'shop', 'cate1', 'cate2', 'cate3'
                ).orderBy(
                    'shop', 'cate1', 'cate2', 'cate3', func.desc('item_sum_amount')
                    )
    ))
# sdf_cumsum_ratio.show(200)

# ABCランク付け
# sdf_amount_abc : 
#   shop, cate1, cate2, cate3, cat4, itmcd, item_sum_amount, shop_cate_amount_all, ratio, cumsum_ratio, abc_rank 
sdf_amount_abc = sdf_cumsum_ratio.withColumn(
    'abc_rank',
    func.when(
        sdf_cumsum_ratio.cumsum_ratio <= param_abc_s, 'S'
    ).when(
        (sdf_cumsum_ratio.cumsum_ratio > param_abc_s) & (sdf_cumsum_ratio.cumsum_ratio <= param_abc_a), 'A'
    ).when(
        (sdf_cumsum_ratio.cumsum_ratio > param_abc_a) & (sdf_cumsum_ratio.cumsum_ratio <= param_abc_b), 'B'
    ).when(
        (sdf_cumsum_ratio.cumsum_ratio > param_abc_b) & (sdf_cumsum_ratio.cumsum_ratio <= param_abc_c), 'C'
    ).otherwise('D')
)

# sdf_amount_abc.show(200)

# >>確認用
# ランクごとの商品数を表示
sdf_rank_item_count = sdf_amount_abc.groupBy(
    'shop', 'cate1', 'cate2', 'cate3', 'abc_rank'
).count().orderBy(
    'shop', 'cate1', 'cate2', 'cate3', 'abc_rank'
)
# sdf_rank_item_count.show()
# <<確認用

out_gdf_popular_amount_abc = s3_write_spark_dataframe_single_file(
    sdf_amount_abc,
    's3://popular-amount-abc'
    )

print("=====================================")
print("job commit")
print("=====================================")
job.commit()

