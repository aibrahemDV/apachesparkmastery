import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object DeployExample {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder.master("yarn-client").appName("DataFrame Excerises")
      .config("spark.sql.shuffle.partitions","150")
      .getOrCreate();


    val employees_schema = StructType(Array(
      StructField("NAME",StringType,true),
      StructField("JOB_TITLE",StringType,true),
      StructField("DEPARTEMENT",StringType,true),
      StructField("FULL_PART_TIME",StringType,true),
      StructField("MONTHLY_HOURLY",StringType,true),
      StructField("TYPICAL_HOURS",IntegerType,true),
      StructField("ANNUAL_SALARY",DoubleType,true),
      StructField("HOURLY_RATE",DoubleType,true)
    ))


    val employees_data = spark.read.format("csv").option("Header","True").schema(employees_schema)
      .load("D:\\Source-Data\\Employee_Data.csv")
      .toDF();


    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").groupBy("JOB_TITLE").agg(expr("count(*)")).show()


  }
}
