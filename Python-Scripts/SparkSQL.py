from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr, broadcast
from pyspark.sql.functions import count, countDistinct, max, min, sum, sumDistinct, avg, dense_rank, rank, asc, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.catalog import Catalog,Database,Table,Column

spark = SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

spark.sql("SELECT 1+10").show()

