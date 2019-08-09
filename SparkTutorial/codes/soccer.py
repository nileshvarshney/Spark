from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession

"""
Find the best stricker in 2016
"""

sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession.builder.appName('Socccer Data analysis').getOrCreate()

# read player and attributes data
player = spark.read.csv(path='../data/player.csv', header=True)
player_attr = spark.read.csv(path='../data/player_attributes.csv', header=True)

print(player.printSchema())
print(player_attr.printSchema())

