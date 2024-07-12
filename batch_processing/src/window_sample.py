from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import  functions as f
import sys


PROJECT = "<PROJECT_ID>"
GCS_BUCKET = "<BUCKET_ID>"
TEMP_BUCKET = "<TEMP_BUCKET_ID"


spark = SparkSession.builder.appName("Window").getOrCreate()

products_csv = "{}/data/products.csv".format(GCS_BUCKET)

# read products
products_df = spark.read.options(delimiter=",", header=True).csv(products_csv)

# window specification
windowSpecProducts = (
    Window.partitionBy(products_df['category'])
    .orderBy(products_df['price'].desc())
)

rankedPriceByCategory = (
    products_df
    .withColumn("price_rank", f.row_number().over(windowSpecProducts))
    .withColumn("min_price", f.min(f.col("price")).over(windowSpecProducts))
    .withColumn("max_price", f.max(f.col("price")).over(windowSpecProducts))
    .withColumn("avg_price", f.avg(f.col("price")).over(windowSpecProducts))
)



print(rankedPriceByCategory.printSchema())
print(rankedPriceByCategory.show(5, False))

spark.stop()