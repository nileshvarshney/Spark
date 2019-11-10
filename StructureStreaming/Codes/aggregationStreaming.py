from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Agregation Spark Streaming").getOrCreate()

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
        .csv("./datasets/droplocation")

    convicationPerBorough = fileStreamDF.groupBy("borough")\
        .agg({"value":"sum"})\
        .withColumnRenamed("sum(value)","convictions")\
        .orderBy("convictions", ascending=False)

    query = convicationPerBorough.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",30)\
        .start().awaitTermination()
