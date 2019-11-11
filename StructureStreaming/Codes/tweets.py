from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import *
import sys

if __name__ == "__main__":
    if len(sys.argv)  != 3:
        print("Usage: spark-submit countHasTags.py <hostname> <post>")
        exit(-1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession.builder.appName("HashTagCounts").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark.readStream.format("socket").option("host", host)\
        .option("port",port).option("includeTimestamp","true").load()

    # words = lines.select(explode(split(lines.value, " ")).alias("word"))

    # def extract_tags(word):
    #     if  word.lower().startswith("#"):
    #         return word
    #     else:
    #         return "nonTag"

    # extract_tags_udf = udf(extract_tags, StringType())
    # resultDF = words.withColumn("tags", extract_tags_udf(words.word))

    # hashTagCounts = resultDF.where(resultDF.tags != "nonTag").groupBy("tags").count().orderBy("count", ascending=False)


    # write data in append mode on console
    query = lines\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",30)\
        .start()\
        .awaitTermination()