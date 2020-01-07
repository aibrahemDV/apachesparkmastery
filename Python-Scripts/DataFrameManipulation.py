from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

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

# DISTINCT RECORDS #
# employees_data.select("JOB_TITLE").distinct().show(truncate=False)
# employees_data.select("DEPARTEMENT","JOB_TITLE").distinct().orderBy("DEPARTEMENT").show(50, truncate=False)

# Frequent Items RECORDS #
# employees_data.freqItems(("DEPARTEMENT", "MONTHLY_HOURLY")).show(truncate=False)

# Filter RECORDS #
# employees_data.select("DEPARTEMENT").distinct().show()
# employees_data.filter(col("DEPARTEMENT") == 'HEALTH').show(truncate=False)
# employees_data.filter(col("DEPARTEMENT") == 'HEALTH').filter(expr("JOB_TITLE like '%ENGINEER%'")).show(truncate=False)
employees_data.filter((col("DEPARTEMENT") == 'HEALTH') & (expr("JOB_TITLE").like('%ENGINEER%'))).show(100)
employees_data.filter((col("DEPARTEMENT") != 'HEALTH') & (expr("JOB_TITLE").like('%ENGINEER%'))).show(100)

# Drop Duplicates RECORDS # employees_data.select("JOB_TITLE", "DEPARTEMENT").dropDuplicates(["JOB_TITLE",
# "DEPARTEMENT"]).orderBy("DEPARTEMENT").show(100, False) employees_data.select("JOB_TITLE", "DEPARTEMENT").orderBy(
# "DEPARTEMENT").show(100, False)

# Handle Null Values #
# employees_data.na.fill(0.0).show(False)
# employees_data.na.fill(0.0, subset=["ANNUAL_SALARY", "HOURLY_RATE"]).na.fill(8.0, subset=["TYPICAL_HOURS"]).show()
# employees_data.na.fill(0.0, subset=["ANNUAL_SALARY", "HOURLY_RATE"]).na.fill(8.0, subset=["TYPICAL_HOURS"]).\
#   na.fill("MISSING", subset=["FULL_PART_TIME"]).show()
#
# employees_data.na.fill({"ANNUAL_SALARY": 0.0, "HOURLY_RATE": 0.0, "TYPICAL_HOURS": 8.0, "FULL_PART_TIME":
# "MISSING"}).show()


# employees_data.na.drop().show()
# employees_data.na.drop("any").show()
# employees_data.na.drop("all").show()
# employees_data.na.drop("all", subset=["TYPICAL_HOURS", "HOURLY_RATE"]).show()

employees_data.filter(col("JOB_TITLE").isNull()).show()
employees_data.filter(col("TYPICAL_HOURS").isNull() & col("HOURLY_RATE").isNull()).show()
