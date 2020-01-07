import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.{ expr, col, column, desc, asc, coalesce}
import org.apache.spark.sql._
import scala.util.matching.Regex


object DataFrameManipulation {
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
      //.repartition(37,col("DEPARTEMENT")).sortWithinPartitions(col("DEPARTEMENT"), col("ANNUAL_SALARY"));



////////////// Define DataFrame //////////////////////////////////////////////////////
/*       employees_data.show(100,false)
       employees_data.show(false)
       employees_data.show(true)
       employees_data.first()*/

      //println(employees_data.first())
      //println(employees_data.head())

/*
      val head_10 = employees_data.head(10)
      for ( i <- 0 to head_10.length-1){
         print(head_10(i));
         print("\n")
      }
*/

/*      val employees_500 = employees_data.limit(500);

      employees_500.show();*/


/////////////// SELECT Columns in different ways ////////////////////////////////////////////////////////////////////
      //employees_data.select("NAME","DEPARTEMENT","JOB_TITLE").show(5,false)
//      employees_data.select(col("NAME"),column("DEPARTEMENT"),col("MONTHLY_HOURLY")).show()

/*      employees_data.select(expr("NAME as EMPLOYEE_FULL_NAME"),
                            expr("case(FULL_PART_TIME) when 'F' then 'FULL_EMPLOYED' " +
                              "   WHEN 'P' then 'PART_TIME' END as EMPLOYMENT_TYPE")).show()*/

//        employees_data.select(expr("NAME as Full_Name"), expr("ANNUAL_SALARY/30 as MONTHLY_SALARY")).show(false)
//          employees_data.selectExpr("NAME as Full_Name", "ANNUAL_SALARY/30 as MONTHLY_SALARY").show(false)

/*
        employees_data.selectExpr("NAME as Full_Name","(ANNUAL_SALARY/30)<2500 as SALARY_INDICATOR",
                                    "HOURLY_RATE*TYPICAL_HOURS as HOURLY_SALARY")
*/


/******************** DISTINCT RECORDS ********************************** */
  //employees_data.select("JOB_TITLE").distinct().show(false)
  //employees_data.select("DEPARTEMENT","JOB_TITLE").distinct().orderBy("DEPARTEMENT").show(50,false)

/********************* Filter Records *************************************/
/*  employees_data.filter(column("DEPARTEMENT")==="HEALTH").show(50)
  employees_data.filter(column("DEPARTEMENT")==="HEALTH").filter(expr("JOB_TITLE like '%ENGINEER%'")).show(false)
  employees_data.filter(expr("DEPARTEMENT=='HEALTH' AND JOB_TITLE like '%ENGINEER%'")).show(false)*/
//  employees_data.filter(col("DEPARTEMENT").equalTo("HEALTH").and(col("JOB_TITLE").like("ENGINEER")))

/******************** Sort DataFrame ********************************** */

      /*      employees_data.sort("ANNUAL_SALARY").show()
      employees_data.orderBy("NAME").show()*/

      /*      employees_data.select("NAME","ANNUAL_SALARY").sort(desc("ANNUAL_SALARY")).show()
            employees_data.select("NAME","ANNUAL_SALARY").orderBy(desc("NAME")).show()*/

      /*      employees_data.sort(col("Annual_salary").asc_nulls_last).show()
            employees_data.sort("Annual_salary".asc_nulls_last).show()*/

      /*      employees_data.select(col("NAME"),col("ANNUAL_SALARY"),expr("ANNUAL_SALARY/30 as MONTHLY_SALARY"))
              .orderBy(expr("ANNUAL_SALARY/30").asc_nulls_last)
              .show(10,false)*/


/******************** Remove Duplicates RECORDS ********************************** */
/*   employees_data.select("JOB_TITLE","DEPARTEMENT").dropDuplicates("JOB_TITLE","DEPARTEMENT")
     .orderBy("DEPARTEMENT").show(100,false)
   employees_data.select("JOB_TITLE","DEPARTEMENT").orderBy("DEPARTEMENT").show(100,false)*/

/******************** Handle Null Values ********************************** */
/*employees_data.show(false)
employees_data.na.fill(0.0).show(false)
employees_data.na.fill(0.0,Seq("ANNUAL_SALARY","HOURLY_RATE")).na.fill(8.0,Seq("TYPICAL_HOURS")).show()
employees_data.na.fill(0.0,Seq("ANNUAL_SALARY","HOURLY_RATE")).na.fill(8.0,Seq("TYPICAL_HOURS")).
  na.fill("MISSING",Seq("FULL_PART_TIME")).show()

employees_data.na.fill(Map("ANNUAL_SALARY" -> 0.0, "HOURLY_RATE" -> 0.0, "TYPICAL_HOURS" -> 8.0,
                           "FULL_PART_TIME" -> "MISSING")).show()*/

/*employees_data.na.drop().show()
employees_data.na.drop("any").show()
employees_data.na.drop("all").show()
employees_data.na.drop("all",Seq("TYPICAL_HOURS","HOURLY_RATE")).show()*/

/*
employees_data.filter(col("JOB_TITLE").isNull).show()
employees_data.filter(col("TYPICAL_HOURS").isNull.and(col("HOURLY_RATE").isNull)).show()*/


   }
}
