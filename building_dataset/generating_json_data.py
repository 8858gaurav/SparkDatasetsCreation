from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import getpass
username = getpass.getuser()
print(username)
spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("local[2]") \
    .getOrCreate()

orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
.format("csv") \
.schema(orders_schema) \
.load("/Users/gauravmishra/Desktop/Generating_data/building_dataset/Input_data/orders.csv")

orders_df.createOrReplaceTempView("orders")

order_items_schema = "order_item_id long,order_item_order_id long,order_item_product_id long,order_item_quantity long,order_item_subtotal float,order_item_product_price float"

order_items_df = spark.read \
.format("csv") \
.schema(order_items_schema) \
.load("/Users/gauravmishra/Desktop/Generating_data/building_dataset/Input_data/order_items.csv")

order_items_df.createOrReplaceTempView("order_items")

customers_schema = "customerid long,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode long"

customers_df =  spark.read \
.format("csv") \
.schema(customers_schema) \
.load("/Users/gauravmishra/Desktop/Generating_data/building_dataset/Input_data/customers.csv")

customers_df.createOrReplaceTempView("customers")

joined_df = spark.sql("""SELECT order_id, order_item_id, customer_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price, customer_fname, customer_lname, city, state, pincode
FROM orders
JOIN order_items ON orders.order_id = order_items.order_item_order_id
JOIN customers ON orders.customer_id = customers.customerid""")

result_df = joined_df.groupBy("order_id","customer_id","customer_fname","customer_lname","city","state","pincode").agg(collect_list(struct("order_item_id", "order_item_product_id","order_item_quantity","order_item_product_price","order_item_subtotal")).alias("line_items")).orderBy("order_id")

result_df.show(20,False)

result_df \
.repartition(1) \
.write \
.format("json") \
.mode("overwrite") \
.option("path","/Users/gauravmishra/Desktop/SparkProject/SparkStreaming/Generating_data/building_dataset/Output_data") \
.save()