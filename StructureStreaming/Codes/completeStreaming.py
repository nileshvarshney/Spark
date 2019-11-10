from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Creatting spark session
    spark = SparkSession\
        .builder\
        .appName("SparkCompleteStreaming")\
        .getOrCreate()

    # defining schema
    schema = StructType([
        StructField("lsoc_code", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("major_category", StringType(), True),
        StructField("minor_category", StringType(), True),
        StructField("value", StringType(), True),
        StructField("month", StringType(), True),
        StructField("year", StringType(), True),
    ])

    # reading streaming data
    fileStreamDF = spark\
                    .readStream\
                    .option("maxFilesPerTrigger",1)\
                    .option("header","true").schema(schema)\
                    .csv("/Users/nilvarshney/github_nilesh/Spark/StructureStreaming/datasets/droplocation")


    print(" ")
    print("Is streaming ", fileStreamDF.isStreaming)

    print("")
    print("Streaming dataframe")
    print(fileStreamDF.printSchema)

    #print(fileStreamDF.show(3))

    # transformation
    recordPerBorough = fileStreamDF.groupBy("borough").count().orderBy("count", ascending=False)

    # print(ConvictionPerDF.show(3))

    # Write Stream  data on console
    query = recordPerBorough.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",30)\
        .start()\
        .awaitTermination()


  

