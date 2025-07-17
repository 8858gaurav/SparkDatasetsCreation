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

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

orders_df = spark \
.read \
.format("json") \
.schema(orders_schema) \
.option("path","/Users/gauravmishra/Desktop/Generating_data/building_dataset/Output_data/part-00000-b6a88d20-2f99-4ab3-a2c5-b5c0b21979c6-c000.json") \
.load()

orders_df.createOrReplaceTempView("orders")
   
spark.sql("select * from orders").show()

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

exploded_orders.show()

exploded_orders.createOrReplaceTempView("exploded_orders")

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

flattened_orders.show()

flattened_orders.createOrReplaceTempView("orders_flattened")

aggregated_orders = spark.sql("""select customer_id, count(distinct(order_id)) as orders_placed, 
count(item_id) as products_purchased,sum(subtotal) as amount_spent 
from orders_flattened group by customer_id""")

aggregated_orders.createOrReplaceTempView("orders_aggregated")

spark.sql("select * from orders_aggregated where customer_id = 256").show()

aggregated_orders \
.repartition(1) \
.write \
.format("csv") \
.mode("overwrite") \
.option("header",True) \
.option("path","/Users/gauravmishra/Desktop/Generating_data/procesing_data/output_data") \
.save()

# 4 different modes are available: overwrite, append, ignore,errorifexists.