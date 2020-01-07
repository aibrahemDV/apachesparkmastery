import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object PartitioningAndSavingData {

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
      .load("D:\\Source-Data\\Employee_Data.csv")
      .toDF();



/******************** Partition DataFrame ********************************** */
    // println(employees_data.rdd.getNumPartitions)
    //println("Number of Partitions: "+ employees_data_partitioned.rdd.partitions.size)
    /*      val employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)
            .load("D:\\Source-Data\\Employee_Data.csv")
            .repartition(10);*/

    /*      println(employees_data.select("DEPARTEMENT").distinct().count())
            println()
            println("Number of Partitions: "+ employees_data_partitioned.rdd.getNumPartitions)

           val employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)
           .load("D:\\Source-Data\\mployee_Data.csv")
           .repartition(col("DEPARTEMENT"));*/

    /*      val employees_data_partitioned = spark.read.format("csv").option("Header","True").schema(employees_schema)
            .load("D:\\Source-Data\\Employee_Data.csv")
            .repartition(15,col("DEPARTEMENT"));

           println()
           println("Number of Partitions: "+ employees_data_partitioned.rdd.getNumPartitions)*/


/*    val employees_data_partitioned = employees_data.repartition(15,col("DEPARTEMENT"))

    println()
    println("Number of Partitions (Original DF): "+ employees_data.rdd.getNumPartitions)
    println()
    println("Number of Partitions (Partitioned DF): "+ employees_data_partitioned.rdd.getNumPartitions)*/


/******************** Save DataFrame data ********************************** */

//------------------------- CSV --------------------------------------------//
/*
      employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")
      employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.csv("D:\\Target-Data\\CSV\\employee_data.csv")
*/

/*
    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
                   .option("sep","|").option("nullValue","UNIDENTIFIED")
                   .format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")

    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
                  .option("sep","|").option("nullValue","UNIDENTIFIED")
                  .csv("D:\\Target-Data\\CSV\\employee_data.csv")
*/

//------------------------- JSON --------------------------------------------//

/*

    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.format("json").save("D:\\Target-Data\\JSON\\employee_data.json")
    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.json("D:\\Target-Data\\JSON\\employee_data.json")
*/


//------------------------- Text --------------------------------------------//


/*
   employees_data.select("NAME","DEPARTEMENT","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
     .partitionBy("DEPARTEMENT","FULL_PART_TIME")
     .format("text").save("D:\\Target-Data\\TXT\\employee_data.txt")

   employees_data.select("NAME","DEPARTEMENT","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
     .partitionBy("DEPARTEMENT","FULL_PART_TIME")
     .mode(SaveMode.Overwrite).text("D:\\Target-Data\\TXT\\employee_data.txt")
*/

   //employees_data.repartition(1).rdd.saveAsTextFile("D:\\Target-Data\\TXT\\employee_data.txt")


//------------------------- Parquet --------------------------------------------//


/*
        employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
                      .format("parquet").save("D:\\Target-Data\\PARQUET\\employee_data.parquet")

        employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
                      .parquet("D:\\Target-Data\\PARQUET\\employee_data.parquet")
*/


//------------------------- JDBC --------------------------------------------//


val prop = new java.util.Properties()
    prop.put("user","sparkusr")
    prop.put("password","sparkusr")
    prop.put("useSSL","false")
    prop.put("allowPublicKeyRetrieval","true")
    prop.put("driver","com.mysql.cj.jdbc.Driver")


    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
      .jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.employees_data", prop)


/*
    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
      .mode(SaveMode.Overwrite).format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/employeesdb")
      .option("user","root")
      .option("dbtable","employeesdb.employees_data")
      .option("useSSL","false")
      .option("allowPublicKeyRetrieval","true")
*/



    val data = spark.read
      .jdbc("jdbc:mysql://localhost:3306/employeesdb","employeesdb.dept_manager", prop)


    data.show()

//--------------------- Partition and Sort output --------------------------------------------//

/*
    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME","DEPARTEMENT").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write
      .partitionBy("DEPARTEMENT","FULL_PART_TIME")
      .option("sep","|").option("nullValue","UNIDENTIFIED")
      .json("D:\\Target-Data\\CSV\\employee_data.csv")
*/



//--------------------- Handle Conflict  --------------------------------------------//
/*    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.mode(SaveMode.Append)
      .option("sep","|").option("nullValue","UNIDENTIFIED")
      .format("csv").save("D:\\Target-Data\\CSV\\employee_data.csv")

    employees_data.select("NAME","JOB_TITLE","FULL_PART_TIME").where(expr("DEPARTEMENT IN ('TRANSPORTN','FIRE')")).write.mode(SaveMode.ErrorIfExists)
      .option("sep","|").option("nullValue","UNIDENTIFIED")
      .csv("D:\\Target-Data\\CSV\\employee_data.csv")*/












  }


  }
