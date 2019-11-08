from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # create spark session
    spark = SparkSession.builder.appName("Append Streaming").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # declare schema defination
    schema = StringType(
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
        .option("schema",schema)\
        .csv("../datasets/droplocation")   

    print(" ")
    print("Is the streaming ready?")
    print(fileStreamDF.isStreaming)

    print(" ")
    print("Schema of input stream")
    print(fileStreamDF.printSchema)

    # trim DF 
    trimmedDF = fileStreamDF.select(
        fileStreamDF.borough,
        fileStreamDF.value,
        fileStreamDF.year,
        fileStreamDF.month
    ).withColumnRenamed("value","conviction")

    # write data
    query = trimmedDF.writeStream

