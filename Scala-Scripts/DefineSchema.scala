import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField,StructType,StringType,IntegerType,LongType,DateType}
import org.apache.spark.sql.Row




object DefineSchema {
   def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)

      val spark = SparkSession.builder.master("local[*]").appName("DataFrame Excerises")
        .config("spark.sql.warehouse.dir", "target/spark-warehouse")
        .getOrCreate();

      import spark.implicits._

      val library_schema = StructType(Array(
         StructField("MONTH_ID",StringType,true),
         StructField("YEAR_ID",IntegerType,true),
         StructField("NUM_SESSIONS",IntegerType,true),
         StructField("CUMULATIVE_SESSIONS",LongType,true)
      ))

      //option 1

/*      val library_data = spark.read.option("Header","True").schema(library_schema)
        .csv("D:\\Source-Data\\Libraries_WiFi_Usage_2011_2014.csv")
        .toDF()
      ;*/

      //option 2

      val library_data = spark.read.format("csv").option("Header","True").schema(library_schema).
        load("D:\\Source-Data\\Libraries_WiFi_Usage_2011_2014.csv")
      .toDF();


      library_data.show(100);
      println(library_data.schema)
      library_data.printSchema();

      // toDF("Column1","Column2","Column3","Column4")



      val status_schema = StructType(Array(
         StructField("Status_ID",IntegerType,false),
         StructField("Status_Description",StringType,false)
      ))

      val statusReference = Seq(Row(100,"WORKING"),
                                Row(200,"DISABLED"),
                                Row(300,"ERROR"),
                                Row(400,null))
      val statusRef_RDD = spark.sparkContext.parallelize(statusReference)
      val statusRef_DF=spark.createDataFrame(statusRef_RDD,status_schema)

      statusRef_DF.show()


   }
}
