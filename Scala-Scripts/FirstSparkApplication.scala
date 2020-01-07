import org.apache.spark.sql.SparkSession

object FirstSparkApplication {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("DataFrame Excerises")
      .config("spark.sql.autoBroadcastJoinThreshold",1)
      .config("spark.sql.crossJoin.enable",true)
      .getOrCreate();

    val x=8

    println(x+3)


  }

}
