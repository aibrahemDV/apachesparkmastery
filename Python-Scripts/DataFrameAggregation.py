from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr, broadcast
from pyspark.sql.functions import count, countDistinct, max, min, sum, sumDistinct, avg, dense_rank, rank, asc, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

sales_schema = StructType([
    StructField("REQ_ID", IntegerType(), True),
    StructField("EMP_ID", IntegerType(), True),
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("PRODUCT_ID", IntegerType(), True),
    StructField("PRICE", IntegerType(), True)
])


customer_schema = StructType([
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE_NUMBER", StringType(), True),
    StructField("ACTIVATION_DATE", StringType(), True)
])

sales_data = spark.read.format("csv").option("Header", "True").schema(sales_schema).load(
    "D:\\Source-Data\\sales_data.csv")


employees_data = spark.read.format("csv").option("Header", "True").option("InferSchema", "true") \
    .load("D:\\Source-Data\\HR_Employees.csv")

customer_data = spark.read.format("csv").option("Header", "True").schema(customer_schema).load(
    "D:\\Source-Data\\customer_info.csv")


trip_data = spark.read.format("csv").option("Header", "True").option("InferSchema", "true").load(
    "D:\\Source-Data\\yellow_tripdata_2017-02.csv")


### Count & countDistinct ####

# print sales_data.count().__str__()
#
# sales_data.select(count("CUSTOMER_ID")).show()
#
# employees_data.select(count("*")).show()
# employees_data.select("Region").show()
# employees_data.select(count("Region")).show()
#
# sales_data.select(countDistinct("CUSTOMER_ID")).show()
# sales_data.groupBy("EMP_ID").agg(count("CUSTOMER_ID")).show()


### Min and Maximum ####
# sales_data.select(max("PRICE")).show()
#
# sales_data.select(min("PRICE")).show()
#
# sales_data.select(min(expr("PRICE * 1000"))).show()
#
# sales_data.select(min(col("PRICE") * 1000)).show()
#
# sales_data.select(min("PRICE").alias("MAX_PRICE"), max("PRICE").alias("MIN_PRICE"), count("*").alias("TOTAL_ORDERS")).show()

# sales_data.groupBy("CUSTOMER_ID").agg(max("PRICE").alias("MAX_PURCHASE"), min("PRICE").alias("MIN_PURCHASE")).show()

### Sum and Average ####
# sales_data.select(sum("PRICE")).show()
# sales_data.select(sumDistinct("PRICE")).show()
# sales_data.groupBy("PRODUCT_ID").agg(sum("PRICE")).orderBy("PRODUCT_ID").show()
# sales_data.groupBy("PRODUCT_ID").sum("PRICE").orderBy("PRODUCT_ID").show()
#
# sales_data.groupBy("CUSTOMER_ID").agg(avg("PRICE").alias("AVERAGE_PURCHASE")).show()
# sales_data.groupBy("CUSTOMER_ID").avg("PRICE").orderBy("CUSTOMER_ID").show()

### EXERCISE ######

# joinCondition = sales_data["CUSTOMER_ID"] == customer_data["CUSTOMER_ID"]
# sales_data.aliass("sales").join(customer_data.aliass("customer"), joinCondition, "inner").\
#     select(expr("sales.CUSTOMER_ID"), expr("sales.PRICE"),
#        expr("customer.FIRST_NAME||' '||customer.LAST_NAME as FULL_NAME"), expr("sales.PRODUCT_ID"),
#        expr("sales.EMP_ID"))\
#     .groupBy("sales.CUSTOMER_ID", "FULL_NAME")\
#     .agg(avg("PRICE").alias("AVG_PURCHASE"), max("PRICE").alias("MAX_PURCHASE"), min("PRICE").alias("MIN_PURCHASE"),
#      count("PRODUCT_ID").alias("PRODUCT_PURCHASED"), countDistinct("EMP_ID").alias("EMPLOYEES_ASSIGNED")).show()


### Analytics ######

tripDF = trip_data.select("VendorID", "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount",
                          "total_amount")

windowDetails = Window.partitionBy("payment_type").orderBy(asc("total_amount"))\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

sumTotalFare = sum("total_amount").over(windowDetails)
maxTotalFare = max("total_amount").over(windowDetails)
fareDenseRank = dense_rank().over(windowDetails)
fareRank = rank().over(windowDetails)

tripDF.select(col("payment_type"), col("total_amount"),
              sumTotalFare.alias("TD_Total_Fare"),
              maxTotalFare.alias("TD_MAX_Fare"),
              fareRank.alias("TD_RANK"),
              fareDenseRank.alias("Dense_Rank")).show()


#### Exercise ######
windowProperties = Window.partitionBy("CUSTOMER_ID").orderBy(desc("PRICE"))\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

sumPurchaseQuantity = sum("PRICE").over(windowProperties)
maxPurchaseQuantity = max("PRICE").over(windowProperties)
purchaseDenseRank = dense_rank().over(windowProperties)
purchaseRank = rank().over(windowProperties)

sales_data.select(col("REQ_ID"), col("CUSTOMER_ID"), col("PRICE"),
                  sumPurchaseQuantity.alias("TD_TOTAL_PURCHASE"),
                  maxPurchaseQuantity.alias("TD_MAX_PURCHASE"),
                  purchaseDenseRank.alias("DENSE_RANK"),
                  purchaseRank.alias("RANK")).orderBy("CUSTOMER_ID").show()


