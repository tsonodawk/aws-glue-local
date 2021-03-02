import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
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
# gds: glue-datastore
gds_receipt = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {'paths': ["s3://test-bucket/sns-receipt_data"]},
    format = "csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx = "gds_receipt"
)

print("=====================================")
print("show debug")
print("=====================================")
df_receipt = gds_receipt.toDF()
# df_receipt.show()

print("=====================================")
print("商品別販売金額のABC分析")
print("=====================================")
# 全合計数
df_sales_amount_all = df_receipt.withColumn(
            "sales_amount", df_receipt.sales_amount.cast('int')
        ).agg(F.sum('sales_amount').alias('amount_all'))

amount_all_list = df_sales_amount_all.rdd.map(list).first()
amount_all = amount_all_list[0]

# 商品別販売数の合計
df_sum_amount = df_receipt.withColumn(
            "sales_amount", df_receipt.sales_amount.cast('int')
        ).select(
            'jan_code', 'sales_amount'
        ).groupBy(
            'jan_code'
        ).agg(
            F.sum('sales_amount').alias('sum_amount')
        ).sort(
            F.desc('sum_amount')
        )

# >> debug
df_sum_amount.show()
print(amount_all)
# << debug

# 構成比作成
df_sum_amount_ratio = df_sum_amount.withColumn('ratio', df_sum_amount.sum_amount / amount_all * 100)
# df_sum_amount_ratio.show()

# 構成比の累作成
df_cumsum_ratio = df_sum_amount_ratio.withColumn('cumsum_ratio',F.sum(df_sum_amount_ratio.ratio).over(Window.partitionBy().orderBy(F.desc('sum_amount'))))
# df_cumsum_ratio.show()
# ABCランク付け
df_amount_ABC = df_cumsum_ratio.withColumn(
                                    'abc_rank',
                                    F.when(
                                        df_cumsum_ratio.cumsum_ratio <= 70, 'A'
                                    ).when(
                                        df_cumsum_ratio.cumsum_ratio >= 90, 'C'
                                    ).otherwise('B')
)

df_amount_ABC.show()

# ランクごとの商品数を表示
df_rank_item_count = df_amount_ABC.groupBy('abc_rank').count()
df_rank_item_count.show()

print("=====================================")
print("S3にファイルを出力する")
print("=====================================")

# 複数ファイル出力を一つにする場合
df_rank_item_count = df_rank_item_count.coalesce(1)

# PySparkのDataFrameをGlueのDataFrameに変換
gdf_rank_item_count = DynamicFrame.fromDF(df_rank_item_count, glueContext, 'gdf_rank_item_count')
gdf_rank_item_count.show()


# s3出力（なぜかバケット直下しかうまくいかない。なぜだ・・・）
out_df_rank_item_count = glueContext.write_dynamic_frame.from_options(
    frame=gdf_rank_item_count,
    connection_type='s3',
    connection_options={
        'path': 's3://test-bucket'
    },
    format='csv',
    transformation_ctx = "out_df_rank_item_count",
)

# writeでも出力は出来たが、やはりバケット直下のみ
# out_df_rank_item_count = gdf_rank_item_count.write(
#     connection_type='s3',
#     connection_options={'path': 's3://test-bucket'},
#     format="csv",
#     # format_options,
#     # accumulator_size
#     )

print("=====================================")
print("job commit")
print("=====================================")
job.commit()
