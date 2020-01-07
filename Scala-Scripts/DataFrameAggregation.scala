import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.{ expr, col, column, desc, asc, coalesce, broadcast}
import org.apache.spark.sql.functions.{count, countDistinct, min, max, sum, sumDistinct, avg, dense_rank, rank}
import org.apache.spark.sql.expressions.Window
import scala.util.matching.Regex

object DataFrameAggregation {
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

    val customer_schema =StructType(Array(
      StructField("CUSTOMER_ID", IntegerType, true),
      StructField("FIRST_NAME", StringType, true),
      StructField("LAST_NAME", StringType, true),
      StructField("EMAIL", StringType, true),
      StructField("PHONE_NUMBER", StringType, true),
      StructField("ACTIVATION_DATE", StringType, true)
    ))


    val sales_data = spark.read.format("csv").option("Header", "True").schema(sales_schema)
      .load("D:\\Source-Data\\sales_data.csv")
      .toDF()

    val employees_data  = spark.read.format("csv").option("Header", "True").option("InferSchema","true")
      .load("D:\\Source-Data\\HR_Employees.csv")
      .toDF()

    val customer_data = spark.read.format("csv").option("Header", "True").schema(customer_schema)
      .load("D:\\Source-Data\\customer_info.csv")
      .toDF()

    val trip_data = spark.read.format("csv").option("Header", "True").option("InferSchema","true")
      .load("D:\\Source-Data\\yellow_tripdata_2017-02.csv")
      .toDF()

    /***** Count & countDistinct *****/

/*    println(sales_data.count());

    sales_data.select(count("CUSTOMER_ID")).show()

    employees_data.select(count("*")).show()
    employees_data.select("Region").show()
    employees_data.select(count("Region")).show()


    sales_data.select(countDistinct("CUSTOMER_ID")).show()

    sales_data.groupBy("EMP_ID").agg(count("CUSTOMER_ID")).show() */

    /***** Minimum & Maximum ******/

/*
    sales_data.select(max(sales_data.col("PRICE"))).show()

    sales_data.select(min("PRICE")).show()

    sales_data.select(min(expr("PRICE * 1000"))).show()

    sales_data.select(min(sales_data.col("PRICE") * 1000 )).show()

    sales_data.select(min("PRICE").as("MAX_PRICE"), max("PRICE").as("MIN_PRICE"), count("*").as("TOTAL_ORDERS")).show()

    sales_data.groupBy("CUSTOMER_ID").agg(max("PRICE").alias("MAX_PURCHASE"), min("PRICE").as("MIN_PURCHASE")).show() */


    /***** Sum & Average ******/

/*    sales_data.select(sum("PRICE").alias("TOTAL_PURCHASE")).show()
    sales_data.select(sumDistinct("PRICE").alias("TOTAL_PURCHASE")).show()
    sales_data.groupBy("PRODUCT_ID").agg(sum("PRICE")).orderBy("PRODUCT_ID").show()
    sales_data.groupBy("PRODUCT_ID").sum("PRICE").orderBy("PRODUCT_ID").show()

    sales_data.groupBy("CUSTOMER_ID").agg(avg("PRICE").alias("AVERAGE_PURCHASE")).show()
    sales_data.groupBy("CUSTOMER_ID").avg("PRICE").orderBy("CUSTOMER_ID").show()*/


    /**** EXCERISE ****************/
/*    val joinCondition = sales_data.col("CUSTOMER_ID") === customer_data.col("CUSTOMER_ID")
    sales_data.as("sales").join(customer_data.as("customer"),joinCondition,"inner").
      select(expr("sales.CUSTOMER_ID"),expr("sales.PRICE"),expr("customer.FIRST_NAME||' '||customer.LAST_NAME as FULL_NAME"),expr("sales.PRODUCT_ID"),expr("sales.EMP_ID"))
      .groupBy("sales.CUSTOMER_ID","FULL_NAME")
      .agg(avg("PRICE").alias("AVG_PURCHASE"),max("PRICE").alias("MAX_PURCHASE"),min("PRICE").alias("MIN_PURCHASE"),
        count("PRODUCT_ID").alias("PRODUCT_PURCHASED"), countDistinct("EMP_ID").alias("EMPLOYEES_ASSIGNED")).show()*/


    /***** Analytics ******/

    val tripDF = trip_data.select("VendorID","passenger_count","trip_distance","payment_type","fare_amount","tip_amount","total_amount")

    val windowDetails = Window.partitionBy("payment_type").orderBy(col("total_amount").asc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val sumTotalFare = sum(col("total_amount")).over(windowDetails)
    val maxTotalFare = max(col("total_amount")).over(windowDetails)
    val fareDenseRank = dense_rank().over(windowDetails)
    val fareRank = rank().over(windowDetails)

    tripDF.select(col("payment_type"),col("total_amount"),
      sumTotalFare.alias("TD_Total_Fare"),
      maxTotalFare.alias("TD_MAX_Fare"),
      fareRank.alias("TD_RANK"),
      fareDenseRank.alias("Dense_Rank")).show()


        val windowProperties = Window.partitionBy("CUSTOMER_ID").orderBy(col("PRICE").desc)
                           .rowsBetween(Window.unboundedPreceding,Window.currentRow)
        val sumPurchaseQuantity = sum(col("PRICE")).over(windowProperties)
        val maxPurchaseQuantity = max(col("PRICE")).over(windowProperties)

        val purchaseDenseRank = dense_rank().over(windowProperties)
        val purchaseRank = rank().over(windowProperties)

        sales_data.select(col("REQ_ID"),col("CUSTOMER_ID"),col("PRICE"),
          sumPurchaseQuantity.alias("TD_TOTAL_PURCHASE"),
          maxPurchaseQuantity.alias("TD_MAX_PURCHASE"),
          purchaseDenseRank.alias("DENSE_RANK"),
          purchaseRank.alias("RANK")).orderBy("CUSTOMER_ID").show()





  }

}
