from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import udf
import time
from datetime import datetime


if __name__ == "__main__":

    # Open spark session
    spark = SparkSession.builder.appName("Spark Streaming and UDF").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # declare schema defination
    schema = StructType(
        [
            StructField("lsoc_code",StringType(),True),
            StructField("borough",StringType(),True),
            StructField("major_category",StringType(),True),
            StructField("minor_category",StringType(),True),
            StructField("value",StringType(),True),
            StructField("year",StringType(),True),
            StructField("month",StringType(),True)   
        ]
    )

    # read the streaming
    fileStreamDF = spark.readStream\
        .option("header","true")\
        .schema(schema)\
        .csv("./datasets/droplocation") 

    def get_timestammp():
        ts = time.time()
        timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%s")
        return timestamp

    add_timestamp_udf = udf(get_timestammp,StringType())

    # add timestamp column to existing dataframe
    fileStreamWithTS = fileStreamDF.withColumn("timestamp", add_timestamp_udf())

    # get only required fields
    trimmedDF = fileStreamWithTS.select("borough","major_category","value","timestamp")

    # write data in append mode on console
    # write data
    query = trimmedDF\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",30)\
        .start()\
        .awaitTermination()


    

