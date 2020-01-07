import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.{ expr, col, column, desc, asc, coalesce, broadcast}
import org.apache.spark.sql._
import scala.util.matching.Regex



object DataFrameJoins {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder.master("local[*]").appName("DataFrame Excerises")
      .config("spark.sql.autoBroadcastJoinThreshold",1)
      .config("spark.sql.crossJoin.enable",true)
      .getOrCreate();



    val sales_schema = StructType(Array(
      StructField("REQ_ID", IntegerType, true),
      StructField("EMP_ID", IntegerType, true),
      StructField("CUSTOMER_ID", IntegerType, true),
      StructField("PRODUCT_ID", IntegerType, true),
      StructField("PRICE", IntegerType, true)
    ))

    val product_schema = StructType(Array(
      StructField("PRODUCT_ID", IntegerType, true),
      StructField("PRODUCT_NAME", StringType, true),
      StructField("MANUFACTURER_NAME", StringType, true),
      StructField("PRODUCT_COLOR", StringType, true)
    ))

    val customer_schema =StructType(Array(
      StructField("CUSTOMER_ID", IntegerType, true),
      StructField("FIRST_NAME", StringType, true),
      StructField("LAST_NAME", StringType, true),
      StructField("EMAIL", StringType, true),
      StructField("PHONE_NUMBER", StringType, true),
      StructField("ACTIVATION_DATE", StringType, true)
    ))


    val sales_data = spark.read.format("csv").option("Header", "True").schema(sales_schema)
      .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\sales_data.csv")
      .toDF();

    val product_data = spark.read.format("csv").option("Header", "True").schema(product_schema)
      .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\product_info.csv")
      .toDF();

    val employees_data  = spark.read.format("csv").option("Header", "True").option("InferSchema","true")
      .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\HR_Employees.csv")
      .toDF();

    val customer_data = spark.read.format("csv").option("Header", "True").schema(customer_schema)
      .load("D:\\\\DV\\\\Courses\\\\Apache-Spark\\\\Sources\\\\intelliji-ws\\\\SparkCourse-v2\\\\src\\\\main\\\\resources\\\\customer_info.csv")
      .toDF();

//    sales_data.show()
//    product_data.show()

/*
    val detailed_sales = sales_data.join(product_data, sales_data.col("PRODUCT_ID")===product_data.col("PRODUCT_ID"))
    detailed_sales.show()
*/


    val joinCondition = sales_data.col("PRODUCT_ID") === product_data.col("PRODUCT_ID")

//    val detailedSales = sales_data.join(product_data,joinCondition,"inner")

//    val detailedSales = sales_data.join(product_data,joinCondition,"left_outer")

//    val detailedSales = sales_data.join(product_data,joinCondition,"right_outer")

//    val detailedSales = sales_data.join(product_data,joinCondition,"outer")

//    val detailedSales = sales_data.join(product_data,joinCondition,"left_semi")

//    val detailedSales = sales_data.join(product_data,joinCondition,"left_anti")

//    val detailedSales = sales_data.crossJoin(product_data)

//   employees_data.select("EmployeeID","LastName","FirstName","ReportsTo").show()

/*   employees_data.as("emp").join(employees_data.as("manager"),joinExprs = expr("emp.EmployeeID=manager.ReportsTo"),"left_outer")
      .select(expr("emp.EmployeeID"),expr("emp.FirstName||' '||emp.LastName as Employee_Name"), expr("manager.FirstName||' '|| manager.LastName as Manager_Name"))
      .show()*/


//    detailedSales.show(100)

//    sales_data.join(broadcast(product_data),joinCondition,"inner").explain(false)


    /* Exercise */
    val joinCondition_Customer = sales_data.col("CUSTOMER_ID") === customer_data.col("CUSTOMER_ID")

    val sales_360_View = sales_data.join(product_data,joinCondition,"inner").join(customer_data,joinCondition_Customer,"inner")
      .select(expr("REQ_ID"),expr("EMP_ID"),expr("PRODUCT_NAME"),expr("PRICE"),expr("FIRST_NAME||' '||LAST_NAME as CUSTOMER_NAME")).show()


  }

 }
