from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark Streaming using SQL").getOrCreate()

    # define schema structure
    schema = StructType(
        [
            StructField("lsoc_code",StringType(),True),
            StructField("borough", StringType(), True),
            StructField("major_category",StringType(),True),
            StructField("minor_category",StringType(),True),
            StructField("value",StringType(),True),
            StructField("year",StringType(),True),
            StructField("month",StringType(),True)
        ]
    )

    # read data
    fileStreamDF = spark.readStream\
        .option("header","true")\
        .option("maxFilesPerTriiger",2)\
        .schema(schema)\
        .csv("/Users/nilvarshney/github_nilesh/Spark/StructureStreaming/datasets/droplocation")

    fileStreamDF.createOrReplaceTempView("LondonCrimeData")
    
    categoryDF = spark.sql(\
        "SELECT major_category, value \
         FROM LondonCrimeData \
        WHERE year = '2016'" )

    convicationByCategory = categoryDF.groupBy("major_category")\
                                      .agg({"value":"sum"}) \
                                      .withColumnRenamed("sum(value)", "convictions") \
                                      .orderBy("convictions", ascending=False)

    query = convicationByCategory.writeStream\
                .outputMode("complete")\
                .format("console")\
                .option("truncate","false")\
                .option("numRows",30)\
                .start().awaitTermination()
