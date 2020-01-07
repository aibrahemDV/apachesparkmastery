from pyspark.sql import SparkSession
from pyspark.sql.functions import col,column,expr
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,IntegerType

spark=SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

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


employees_data = spark.read.format("csv").option("Header", "True").schema(employees_schema).\
    load("D:\\Source-Data\\Employee_Data.csv");

# Add new column #
employees_data.withColumn("MANAGEMENT_FLAG", expr("DEPARTEMENT like '%MGMNT%' ")).select("DEPARTEMENT","MANAGEMENT_FLAG").show(100)
hourly_paid_employee = employees_data.filter(col("HOURLY_RATE") > 0.0).withColumn("HOURLY_PAYMENT",col("HOURLY_RATE") * col("TYPICAL_HOURS")).\
    select("NAME", "JOB_TITLE", "DEPARTEMENT", "TYPICAL_HOURS", "HOURLY_RATE", "HOURLY_PAYMENT").show()

# Rename column #
employees_data.withColumnRenamed("NAME","EMPLOYEE_NAME").withColumnRenamed("JOB_TITLE","TITLE").show(truncate=False)

# Drop column #
employees_data.drop("MONTHLY_HOURLY").show(50)

# case column datatype #
employees_data.select(expr("ANNUAL_SALARY/30"), expr("ANNUAL_SALARY/30 as MONTHLY_SALARY").cast("Integer")).printSchema()
