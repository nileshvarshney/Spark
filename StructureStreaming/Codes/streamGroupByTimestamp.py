from pyspark.sql import SparkSession
from pyspark.sql.types  import StringType, StructField, StructType
from pyspark.sql.functions import udf
from datetime import datetime
from time import time

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Stream data groupby timestamp').getOrCreate()

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
        .csv("../datasets/droplocation") 

    # Custom function to create the timestamp
    def get_timestamp():
        ts = time()
        timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        return timestamp

    # udf function
    timestamp_udf = udf(get_timestamp, StringType())

    # hookup timestamp with the main data
    fileStreamWithTS = fileStreamDF.withColumn("timestamp", timestamp_udf())

    # transformation
    convictionPerTimestamp = fileStreamWithTS\
                    .groupBy("timestamp")\
                    .agg({"value":"sum"})\
                    .withColumnRenamed("sum(value)","convictions")\
                    .orderBy("convictions",asscending=False)

     # write data in append mode on console
    query = convictionPerTimestamp\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",30)\
        .start()\
        .awaitTermination()
