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
    "D:\\Source-Data\\Employee_Data.csv")



########################## Partition DataFrame #################################################
print(employees_data.rdd.getNumPartitions)

employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)\
        .load("D:\\Source-Data\\Employee_Data.csv")\
        .repartition(10)

print(employees_data.select("DEPARTEMENT").distinct().count())
print()
print("Number of Partitions: "+ employees_data_partitioned.rdd.getNumPartitions)

# employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)\
#     .load("D:\\Source-Data\\Employee_Data.csv")\
#     .repartition(col("DEPARTEMENT"))

# employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)\
#     .load("D:\\Source-Data\\Employee_Data.csv")\
#     .repartition(15,col("DEPARTEMENT"))

print()
print("Number of Partitions: "+ employees_data_partitioned.rdd.getNumPartitions)


employees_data_partitioned = employees_data.repartition(15,col("DEPARTEMENT"))

print()
print("Number of Partitions (Original DF): "+ employees_data.rdd.getNumPartitions)
print()
print("Number of Partitions (Partitioned DF): "+ employees_data_partitioned.rdd.getNumPartitions)


############################## Save DataFrame data ################################

#--------------------------------- CSV --------------------------------------------//

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")
employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.csv("D:\\Target-Data\\CSV\\employee_data.csv")


employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .option("sep","|").option("nullValue","UNIDENTIFIED")\
    .format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .option("sep","|").option("nullValue","UNIDENTIFIED")\
    .csv("D:\\Target-Data\\CSV\\employee_data.csv")


#---------------------------------  JSON --------------------------------------------//



employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.format("json").save("D:\\Target-Data\\JSON\\employee_data.json")
employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.json("D:\\Target-Data\\JSON\\employee_data.json")



#--------------------------------- Text --------------------------------------------//


employees_data.select("NAME","DEPARTEMENT","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .partitionBy("DEPARTEMENT","FULL_PART_TIME")\
    .format("text").save("D:\\Target-Data\\TXT\\employee_data.txt")

employees_data.select("NAME","DEPARTEMENT","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .partitionBy("DEPARTEMENT","FULL_PART_TIME")\
    .mode("Overwrite").text("D:\\Target-Data\\TXT\\employee_data.txt")


employees_data.repartition(1).rdd.saveAsTextFile("D:\\Target-Data\\TXT\\employee_data.txt")


#---------------------------------  Parquet --------------------------------------------//


employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .format("parquet").save("D:\\Target-Data\\PARQUET\\employee_data.parquet")

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .parquet("D:\\Target-Data\\PARQUET\\employee_data.parquet")


##------------------------- JDBC --------------------------------------------//


props = {"user":"root","useSSL":"false","allowPublicKeyRetrieval":"true","driver":"com.mysql.cj.jdbc.Driver"}

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')"))\
        .write.mode("Append").jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.employees_data", properties=props )


data = spark.read.jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.dept_manager", properties=props)


data.show()

##-------------------------  Partition and Sort output --------------------------------------------//


employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME","DEPARTEMENT").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write\
    .partitionBy("DEPARTEMENT","FULL_PART_TIME")\
    .option("sep","|").option("nullValue","UNIDENTIFIED")\
    .json("D:\\Target-Data\\CSV\\employee_data.csv")


##-------------------------   Handle Conflict  --------------------------------------------//

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.mode(SaveMode.Append)\
    .option("sep","|").option("nullValue","UNIDENTIFIED")\
    .format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")

employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.mode(SaveMode.ErrorIfExists)\
    .option("sep","|").option("nullValue","UNIDENTIFIED")\
    .csv("D:\\Target-Data\\CSV\\employee_data.csv")



