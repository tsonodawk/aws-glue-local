from pyspark import SparkContext
from pyspark.sql import SparkSession


def main():
    spark_context = SparkContext()
    spark = SparkSession(spark_context)

    data_frame = spark.sql('SELECT "Hello, World!!!" AS message')
    data_frame.show()


if __name__ == '__main__':
    main()
