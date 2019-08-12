from pyspark import SQLContext, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,LongType,StructType
from pyspark.sql import Row

sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession(sc)

lines = sc.textFile('../data/students.txt')
parts  = lines.map(lambda l: l.split(","))
students = parts.map(lambda r: Row(
    name=r[0],
    math=int(r[1]),
    english=int(r[2]),
    science=int(r[3])))

# implicit Schema
students_df = spark.createDataFrame(students)
print(students_df.printSchema())
print(students_df.columns)

# Explicit schema
schema = StructType(
    [StructField('name',StringType(), True),
    StructField('math',LongType(), True),
    StructField('english',LongType(), True),
    StructField('science',LongType(), True)]
)

exp_students_df = spark.createDataFrame(students, schema)
# print(exp_students_df.collect())
print(exp_students_df.printSchema())
print(exp_students_df.columns)
print(exp_students_df.show())

exp_students_df.createOrReplaceTempView('exp_students')
spark.sql("select * from exp_students").show()

''' Output
root
 |-- english: long (nullable = true)
 |-- math: long (nullable = true)
 |-- name: string (nullable = true)
 |-- science: long (nullable = true)

None
['english', 'math', 'name', 'science']
root
 |-- name: string (nullable = true)
 |-- math: long (nullable = true)
 |-- english: long (nullable = true)
 |-- science: long (nullable = true)

None
['name', 'math', 'english', 'science']
+-----+----+-------+-------+
| name|math|english|science|
+-----+----+-------+-------+
|Emily|  44|     55|     78|
| Andy|  47|     34|     89|
| Rick|  55|     78|     55|
|Aaron|  66|     34|     98|
+-----+----+-------+-------+

None
+-----+----+-------+-------+
| name|math|english|science|
+-----+----+-------+-------+
|Emily|  44|     55|     78|
| Andy|  47|     34|     89|
| Rick|  55|     78|     55|
|Aaron|  66|     34|     98|
+-----+----+-------+-------+


Process finished with exit code 0
'''




