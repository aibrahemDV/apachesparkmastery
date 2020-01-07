from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

spark = SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

sales_schema = StructType([
    StructField("REQ_ID", IntegerType(), True),
    StructField("EMP_ID", IntegerType(), True),
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("PRODUCT_ID", IntegerType(), True),
    StructField("PRICE", IntegerType(), True)
])

product_schema = StructType([
    StructField("PRODUCT_ID", IntegerType(), True),
    StructField("PRODUCT_NAME", StringType(), True),
    StructField("MANUFACTURER_NAME", StringType(), True),
    StructField("PRODUCT_COLOR", StringType(), True)
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

product_data = spark.read.format("csv").option("Header", "True").schema(product_schema).load(
    "D:\\Source-Data\\product_info.csv")

employees_data = spark.read.format("csv").option("Header", "True").option("InferSchema", "true") \
    .load("D:\\Source-Data\\HR_Employees.csv")


customer_data = spark.read.format("csv").option("Header", "True").schema(customer_schema).load(
    "D:\\Source-Data\\customer_info.csv")

# sales_data.show()
# product_data.show()


# detailed_sales = sales_data.join(product_data, sales_data["PRODUCT_ID"] == product_data["PRODUCT_ID"])

joinCondition = sales_data["PRODUCT_ID"] == product_data["PRODUCT_ID"]
# detailedSales = sales_data.join(product_data, joinCondition, "inner")

# detailedSales = sales_data.join(product_data, joinCondition, "left_outer")

# detailedSales = sales_data.join(product_data, joinCondition, "right_outer")

# detailedSales = sales_data.join(product_data, joinCondition, "outer")

# detailedSales = sales_data.join(product_data, joinCondition, "left_semi")

# detailedSales = sales_data.join(product_data, joinCondition, "left_anti")


employees_data.select("EmployeeID","LastName","FirstName","ReportsTo").show()
employees_data.alias("emp").join(
    employees_data.alias('manager'), expr( "emp.EmployeeID=manager.ReportsTo"), "left_outer").select(expr(
"emp.EmployeeID"), expr("emp.FirstName||' '||emp.LastName as Employee_Name"), expr("manager.FirstName||' '||manager.LastName as Manager_Name")).show()

sales_data.join(broadcast(product_data), joinCondition, "inner").explain()

# detailedSales.show(30)

# Exercise #

joinCondition_Customer = sales_data["CUSTOMER_ID"] == customer_data["CUSTOMER_ID"]


sales_360_View = sales_data.join(product_data, joinCondition, "inner").join(customer_data, joinCondition_Customer,"inner")\
    .select(expr("REQ_ID"), expr("EMP_ID"), expr("PRODUCT_NAME"), expr("PRICE"),
            expr("FIRST_NAME||' '||LAST_NAME as CUSTOMER_NAME")).show()
