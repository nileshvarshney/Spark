import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

############################################
# accept command line arguments
############################################
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage : spark-submit wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)

host = sys.argv[1]
port = sys.argv[2]

############################################
# Create Spark Session
############################################
spark = SparkSession.builder.appName("Word Count Streaming").getOrCreate()

spark.sparkContext.setLogLevel("Error")

############################################
# Read data from command line streaming
############################################
lines = spark.readStream.format('socket').option('host', host).option('port',port).load()

############################################
# Transformation
############################################
words = lines.select(
    explode(
        split(lines.value, ' ')
    ).alias('word'))

wordCounts = words.groupBy('word').count()

############################################
# Write  to console
############################################
query = wordCounts.writeStream.outputMode('complete').format('console').start()

query.awaitTermination()



