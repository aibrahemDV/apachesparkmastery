import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ expr, col, column}
import org.apache.spark.sql.functions.{desc, asc,round}


object DataFrameAlter {
   def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)

      val spark = SparkSession.builder.master("local[*]").appName("DataFrame Excerises")
        .config("spark.sql.warehouse.dir", "target/spark-warehouse")
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
        .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\Employee_Data.csv")
        .toDF();

/************************* Add new Column *******************************************/
      employees_data.withColumn("MANAGEMENT_FLAG",expr("DEPARTEMENT like '%MGMNT%' ")).
        select("DEPARTEMENT","MANAGEMENT_FLAG").show(100)
      val hourly_paid_employee = employees_data.filter(col("HOURLY_RATE") > 0.0).withColumn("HOURLY_PAYMENT",col("HOURLY_RATE") * col("TYPICAL_HOURS"))
      .select("NAME","JOB_TITLE","DEPARTEMENT","TYPICAL_HOURS","HOURLY_RATE","HOURLY_PAYMENT")

      hourly_paid_employee.show(50)

/************************* Rename Column *******************************************/
     employees_data.withColumnRenamed("NAME","EMPLOYEE_NAME").withColumnRenamed("JOB_TITLE","TITLE").show(false)

/************************* Drop Column *******************************************/

    employees_data.drop("MONTHLY_HOURLY").show(50)

/************************* Cast Column DataType *******************************************/
    employees_data.select(expr("ANNUAL_SALARY/30"), expr("ANNUAL_SALARY/30 as MONTHLY_SALARY").cast("Integer")).printSchema()

   }
}
