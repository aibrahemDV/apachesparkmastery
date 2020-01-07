from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
#from mysql.connector import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///D:/Source-Data/mysql-connector-java-8.0.18.jar pyspark-shell'

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
    "D:\\DV\\Courses\\Apache-Spark\\Sources\\intelliji-ws\\SparkCourse-v2\\src\\main\\resources\\Employee_Data.csv")


##------------------------- JDBC --------------------------------------------//


props = {"user":"root","useSSL":"false","allowPublicKeyRetrieval":"true","driver":"com.mysql.cj.jdbc.Driver"}

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')"))\
        .write.mode("Append").jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.employees_data", properties=props )
      # .jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.employees_data", prop)



data = spark.read.jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.dept_manager", properties=props)


data.show()