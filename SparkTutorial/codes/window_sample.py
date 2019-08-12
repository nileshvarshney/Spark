from pyspark import SQLContext, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import  functions as func
import sys

sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession(sc)


# read product file
products = spark.read.csv('../data/products.csv', header=True)
print(products.show(3))

window_spec1 = Window.partitionBy(products['category'])\
    .orderBy(products['price'].desc())

price_rank = func.rank().over(window_spec1)
product_rank = products.select(
    products['product'],
    products['category'],
    products['price'],
).withColumn('rank', price_rank)

print(product_rank.show())

# row bbetween -1 and 1
window_spec2 = Window.partitionBy(products['category'])\
    .orderBy(products['price'].desc())\
    .rowsBetween(-1,1)

product_max = products.select(
    products['product'],
    products['category'],
    products['price'],
).withColumn('max_price', func.max(products['price']).over(window_spec2))

print(product_max.show())

# difference from max product cost to current
window_spec3 = Window.partitionBy(products['category'])\
    .orderBy(products['price'].desc())\
    .rangeBetween(-sys.maxsize, sys.maxsize)

product_diff = products.select(
    products['product'],
    products['category'],
    products['price'],
).withColumn('price_diff', func.max(products['price']).over(window_spec3) - products['price'] )

print(product_diff.show())

'''
+----------+--------+-----+
|   product|category|price|
+----------+--------+-----+
|Samsung TX|  Tablet|  999|
|Samsung JX|  Mobile|  799|
|Redmi Note|  Mobile|  399|
+----------+--------+-----+
only showing top 3 rows

None
+----------+--------+-----+----+
|   product|category|price|rank|
+----------+--------+-----+----+
|    iPhone|  Mobile|  999|   1|
|Samsung JX|  Mobile|  799|   2|
|Redmi Note|  Mobile|  399|   3|
|   OnePlus|  Mobile|  356|   4|
|        Mi|  Mobile|  299|   5|
|  Micromax|  Mobile|  249|   6|
|Samsung TX|  Tablet|  999|   1|
|      iPad|  Tablet|  789|   2|
|    Lenovo|  Tablet|  499|   3|
|        Xu|  Tablet|  267|   4|
+----------+--------+-----+----+

None
+----------+--------+-----+---------+
|   product|category|price|max_price|
+----------+--------+-----+---------+
|    iPhone|  Mobile|  999|      999|
|Samsung JX|  Mobile|  799|      999|
|Redmi Note|  Mobile|  399|      799|
|   OnePlus|  Mobile|  356|      399|
|        Mi|  Mobile|  299|      356|
|  Micromax|  Mobile|  249|      299|
|Samsung TX|  Tablet|  999|      999|
|      iPad|  Tablet|  789|      999|
|    Lenovo|  Tablet|  499|      789|
|        Xu|  Tablet|  267|      499|
+----------+--------+-----+---------+

None
+----------+--------+-----+----------+
|   product|category|price|price_diff|
+----------+--------+-----+----------+
|    iPhone|  Mobile|  999|       0.0|
|Samsung JX|  Mobile|  799|     200.0|
|Redmi Note|  Mobile|  399|     600.0|
|   OnePlus|  Mobile|  356|     643.0|
|        Mi|  Mobile|  299|     700.0|
|  Micromax|  Mobile|  249|     750.0|
|Samsung TX|  Tablet|  999|       0.0|
|      iPad|  Tablet|  789|     210.0|
|    Lenovo|  Tablet|  499|     500.0|
|        Xu|  Tablet|  267|     732.0|
+----------+--------+-----+----------+
'''