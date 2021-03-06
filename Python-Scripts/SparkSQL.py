from pyspark.sql import SparkSession
from pyspark.sql.functions import col, column, expr, broadcast
from pyspark.sql.functions import count, countDistinct, max, min, sum, sumDistinct, avg, dense_rank, rank, asc, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.catalog import Catalog,Database,Table,Column

spark = SparkSession.builder.master("local[*]").appName("Hello Spark").getOrCreate()

spark.sql("SELECT 1+10").show()

spark = SparkSession.builder().master("local[*]").appName("SparkSQL Exercises")\
    .config("spark.sql.warehouse.dir","D:\\spark-warehouse")\
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")\
    .getOrCreate()

sales_data = spark.read.format("csv").option("Header", "True").option("InferSchema","True")\
    .load("file:\\D:\\Source-Data\\sales_data.csv")\
    .toDF()
employees_data  = spark.read.format("csv").option("Header", "True").option("InferSchema","true")\
    .load("D:\\Source-Data\\HR_Employees.csv")\
    .toDF()



##****** Database Management **************/
spark.sql("SELECT current_database()").show()
spark.sql("CREATE DATABASE sparkMatsery")
spark.sql("SHOW DATABASES").show()

spark.sql("USE sparkMatsery")
spark.sql("SELECT CURRENT_DATABASE()").show()

spark.sql("DROP DATABASE IF EXISTS sparkMatsery")
spark.sql("SELECT CURRENT_DATABASE()").show()

##****** create and replace views **************/

## Normal Temp Views
sales_data.groupBy("CUSTOMER_ID").agg(expr("COUNT(*) as number_of_orders"),expr("SUM(PRICE) as total_purchases"))\
    .createTempView("Customer_sales")


sales_data.select("REQ_ID","CUSTOMER_ID","PRICE").createOrReplaceTempView("Customer_sales")

spark.sql("SELECT * FROM customer_sales").show()

spark.sql("SHOW TABLES").show()



## Global Temp Views
employees_data.select(expr("EmployeeID"),expr("LastName||' '||FirstName as Employee_Full_Name"),expr("HireDate"))\
    .createOrReplaceTempView("employee_details")

spark.sql("""SELECT * FROM employee_details""").show()

spark2 = SparkSession.builder().master("local[*]").appName("SparkSQL Exercises_Session_2")\
    .getOrCreate()

spark2.sql("""SELECT * FROM employee_details""").show()


##**** Exercise *****************/


employees_data.select("EmployeeID","LastName","FirstName","ReportsTo").show()
employees_data.alias("emp").join(
    employees_data.alias('manager'), expr( "emp.EmployeeID=manager.ReportsTo"), "left_outer").select(expr(
"emp.EmployeeID"), expr("emp.FirstName||' '||emp.LastName as Employee_Name"), expr("manager.FirstName||' '||manager.LastName as Manager_Name"))\
    .createOrReplaceTempView("Employee_Details")

spark.sql("SELECT * FROM Employee_Details").show()



##********** managed tables  ********************/

sales_data.write.saveAsTable("Sales_table")
spark.sql("SELECT * FROM Sales_table").show()

sales_data.filter(expr("PRODUCT_ID IN (12,15)")).write.mode("Overwrite").saveAsTable("Sales_table")
spark.sql("SELECT * FROM Sales_table").show()

sales_data.write.partitionBy("EMP_ID").saveAsTable("Sales_table")
sales_data.write.mode("Overwrite").partitionBy("EMP_ID").saveAsTable("Sales_table")


spark.sql("SHOW PARTITIONS sales_table").show()

sales_data.filter(expr("PRODUCT_ID IN (12,15)")).select("REQ_ID","CUSTOMER_ID","PRICE")\
    .write.mode("Overwrite").saveAsTable("Sales_table")
spark.sql("SELECT * FROM Sales_table").show()




##******** Unmanaged tables ******************/
spark.sql(
          """CREATE TABLE sales_data_temp (
            |REQ_ID integer,
            |EMP_ID integer,
            |CUSTOMER_ID integer,
            |PRODUCT_ID integer,
            |PRICE integer COMMENT 'Product Price including VAT')
            |USING csv OPTIONS(header true, path 'D:\\Source-Data\\sales_data.csv')
            |""".stripMargin)
spark.sql("SELECT * FROM sales_data_temp").show()

spark.sql(
         """CREATE TABLE IF NOT EXISTS sales_data_temp (
        |REQ_ID integer,
        |EMP_ID integer,
        |CUSTOMER_ID integer,
        |PRODUCT_ID integer,
        |PRICE integer COMMENT 'Product Price including VAT')
        |USING csv OPTIONS(header true, path 'D:\\Source-Data\\sales_data.csv')
        |COMMENT 'Sales Table, contain sales data per employee, customer, and product'
        |""".stripMargin)
spark.sql("SELECT * FROM sales_data_temp").show()


sales_data.select("CUSTOMER_ID","PRODUCT_ID","PRICE").write.option("path","D:\\Source-Data\\sales_data_unmanaged.csv")\
    .saveAsTable("sales_data_unmanaged")
spark.sql("SELECT * FROM sales_data_unmanaged").show()


sales_data.select("REQ_ID","EMP_ID","PRICE").write.mode("Overwrite").option("path","D:\\Source-Data\\sales_data_unmanaged_v2.csv")\
    .saveAsTable("sales_data_unmanaged")

spark.sql("SELECT * FROM sales_data_unmanaged").show()

spark.catalog.listTables().show()


spark.sql("CREATE TABLE IF NOT EXISTS sales_emp_101 USING csv AS " +
      "       SELECT * FROM Sales_data_temp where EMP_ID=101")
spark.sql("SELECT * FROM sales_emp_101").show()

spark.sql("DESCRIBE sales_data_unmanaged").show(false)


##******* CATALOG Functions ****************/

spark.catalog.listTables().show()

spark.catalog.listColumns("sales_table").show()
spark.catalog.listColumns("default","sales_table").show()

spark.catalog.listDatabases().show()

spark.sql("CREATE DATABASE sparkMatsery")
spark.catalog.setCurrentDatabase("sparkMatsery")
print(spark.catalog.currentDatabase)

spark.catalog.createTable("sales_table_catalog","D:\\Source-Data\\sales_data_unmanaged_v2.csv")
spark.sql("SELECT * FROM sales_table_catalog").show()

