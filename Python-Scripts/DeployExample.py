from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

employees_schema = StructType([
    StructField("NAME", StringType(), True),
    StructField("JOB_TITLE", StringType(), True),
    StructField("DEPARTEMENT", StringType(), True),
    StructField("FULL_PART_TIME", StringType(), True),
    StructField("MONTHLY_HOURLY", StringType(), True),
    StructField("TYPICAL_HOURS", IntegerType(), True),
    StructField("ANNUAL_SALARY", DoubleType(), True),
    StructField("HOURLY_RATE", DoubleType(), True)
])

employees_data = spark.read.format("csv").option("Header", "True").schema(employees_schema). \
    load(
    "D:\\Source-Data\\Employee_Data.csv")

employees_data.select("NAME", "JOB_TITLE", "FULL_PART_TIME").groupBy("JOB_TITLE").agg(expr("count(*)")).show()
