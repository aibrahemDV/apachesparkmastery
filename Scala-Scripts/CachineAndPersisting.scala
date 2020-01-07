import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.reflect.internal.util.TableDef.Column


object CachineAndPersisting {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder.master("local[*]").appName("DataFrame Excerises")
      .config("spark.sql.shuffle.partitions","150")
      .getOrCreate();

    //.config("spark.sql.warehouse.dir", "target/spark-warehouse")


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
      .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\Employee_Data.csv")
      .toDF();

    /*** Caching **********************/
    employees_data.cache()

    /******** Persisting ****************/
    employees_data.persist(StorageLevel.MEMORY_AND_DISK)
    employees_data.persist(StorageLevel.DISK_ONLY)

    println(employees_data.rdd.getNumPartitions)


    val partition_employees_1   = employees_data.repartition(4)
    val select_employees_1      = partition_employees_1.select("NAME","JOB_TITLE","DEPARTEMENT")
    val filter_employees        = select_employees_1.filter(expr("DEPARTEMENT == 'FIRE'"))
    val partition_employees_2   = filter_employees.repartition(6)
    partition_employees_2.count()


  }



}
