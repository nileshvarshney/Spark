from pyspark import SparkContext,SQLContext
from pyspark.sql import SparkSession

sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession(sc)

airlines = spark.read.csv('../data/airlines.csv', header=True)
airlines.printSchema()
'''
root
 |-- Code: string (nullable = true)
 |-- Description: string (nullable = true)
 '''
airlines.createOrReplaceTempView('airlines')
spark.sql("select * from airlines").show(3)

flights = spark.read.csv('../data/flights.csv', header=True)
print(flights.columns)

print(flights.count(), airlines.count())

# count the no of observation
flights.createOrReplaceTempView('flights')
'''
['date', 'airlines', 'flight_number', 'origin', 'destination', 'departure', 
'departure_delay', 'arrival', 'arrival_delay', 'air_time', 'distance']

root
 |-- date: string (nullable = true)
 |-- airlines: string (nullable = true)
 |-- flight_number: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- destination: string (nullable = true)
 |-- departure: string (nullable = true)
 |-- departure_delay: string (nullable = true)
 |-- arrival: string (nullable = true)
 |-- arrival_delay: string (nullable = true)
 |-- air_time: string (nullable = true)
 |-- distance: string (nullable = true)
'''

flights_counts = spark.sql("SELECT COUNT(*) FROM flights")
airlines_counts = spark.sql("SELECT COUNT(*) FROM airlines")
print(flights_counts.collect()[0][0],airlines_counts.collect()[0][0])

spark.sql("select * from flights").show(3)

# total distiance
total_distance_df = spark.sql("SELECT distance from flights")\
    .agg({"distance":"sum"})\
    .withColumnRenamed("sum(distance)","tatal_distance")

print(total_distance_df.collect()[0][0])

print(spark.sql("select sum(distance) AS total_distance from flights").collect()[0][0])

# find flight delay in 2014
all_delay_2014 = \
    spark.sql("select date, airlines, flight_number, departure_delay" +
          " from flights where year(date) = 2014 and departure_delay > 0")

all_delay_2014.show(5)

all_delay_2014.createOrReplaceTempView("all_delay_2014")

all_delay_2014.orderBy(all_delay_2014.departure_delay.desc()).show(5)

# count the total no of flights delay in 2014
delay_counts = spark.sql("select count(*) as `delayed_flights` from all_delay_2014")
delay_counts.show()
print(delay_counts.collect()[0][0])

delay_percent = delay_counts.collect()[0][0] / flights_counts.collect()[0][0] * 100
print("Delay percent :" + str(delay_percent))

# delay per airlines
delay_per_airline = spark.sql(
    "select airlines, departure_delay from flights"
)\
    .groupBy(flights.airlines)\
    .agg({"departure_delay":"avg"})\
    .withColumnRenamed("avg(departure_delay)","avg_departure_delay")

delay_per_airline.orderBy(delay_per_airline.avg_departure_delay.desc()).show()

delay_per_airline.createOrReplaceTempView("delay_per_airline")

spark.sql("select *  from delay_per_airline order by avg_departure_delay desc").show()

# spark.sql("select * from airlines").show()
#
spark.sql("select Description,airlines,avg_departure_delay from airlines join delay_per_airline " +
          " on airlines.code = delay_per_airline.airlines order by avg_departure_delay desc"
          ).show()

# complete output
'''
root
 |-- Code: string (nullable = true)
 |-- Description: string (nullable = true)

+-----+--------------------+
| Code|         Description|
+-----+--------------------+
|19031|Mackey Internatio...|
|19032|Munz Northern Air...|
|19033|Cochise Airlines ...|
+-----+--------------------+
only showing top 3 rows

['date', 'airlines', 'flight_number', 'origin', 'destination', 'departure', 'departure_delay', 'arrival', 'arrival_delay', 'air_time', 'distance']
476881 1579
476881 1579
+----------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+
|      date|airlines|flight_number|origin|destination|departure|departure_delay|arrival|arrival_delay|air_time|distance|
+----------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+
|2014-04-01|   19805|            1|   JFK|        LAX|     0854|          -6.00|   1217|         2.00|  355.00| 2475.00|
|2014-04-01|   19805|            2|   LAX|        JFK|     0944|          14.00|   1736|       -29.00|  269.00| 2475.00|
|2014-04-01|   19805|            3|   JFK|        LAX|     1224|          -6.00|   1614|        39.00|  371.00| 2475.00|
+----------+--------+-------------+------+-----------+---------+---------------+-------+-------------+--------+--------+
only showing top 3 rows

379052917.0
379052917.0
+----------+--------+-------------+---------------+
|      date|airlines|flight_number|departure_delay|
+----------+--------+-------------+---------------+
|2014-04-01|   19805|            2|          14.00|
|2014-04-01|   19805|            4|          25.00|
|2014-04-01|   19805|            6|         126.00|
|2014-04-01|   19805|            7|         125.00|
|2014-04-01|   19805|            8|           4.00|
+----------+--------+-------------+---------------+
only showing top 5 rows

+----------+--------+-------------+---------------+
|      date|airlines|flight_number|departure_delay|
+----------+--------+-------------+---------------+
|2014-04-18|   19977|          560|          99.00|
|2014-04-18|   20366|         5819|          99.00|
|2014-04-17|   19393|          661|          99.00|
|2014-04-30|   19393|         3594|          99.00|
|2014-04-18|   20366|         4928|          99.00|
+----------+--------+-------------+---------------+
only showing top 5 rows

+---------------+
|delayed_flights|
+---------------+
|         179015|
+---------------+

179015
Delay percent :37.53871510922012
+--------+-------------------+
|airlines|avg_departure_delay|
+--------+-------------------+
|   19393| 13.429567657134724|
|   20366| 12.296210112379818|
|   19977|  8.818392620527979|
|   20436|  8.716275167785234|
|   20409|   8.31110357194785|
|   20398|  7.372135487994157|
|   21171|  6.989682212133719|
|   19805|  6.733031255779545|
|   20304|   6.05227892794094|
|   19790|  5.597661140117859|
|   20437|  5.110621095185594|
|   20355| 3.9925874044242105|
|   19930|-0.6991515343747522|
|   19690|-2.1981308411214955|
+--------+-------------------+

+--------+-------------------+
|airlines|avg_departure_delay|
+--------+-------------------+
|   19393| 13.429567657134724|
|   20366| 12.296210112379818|
|   19977|  8.818392620527979|
|   20436|  8.716275167785234|
|   20409|   8.31110357194785|
|   20398|  7.372135487994157|
|   21171|  6.989682212133719|
|   19805|  6.733031255779545|
|   20304|   6.05227892794094|
|   19790|  5.597661140117859|
|   20437|  5.110621095185594|
|   20355| 3.9925874044242105|
|   19930|-0.6991515343747522|
|   19690|-2.1981308411214955|
+--------+-------------------+

+--------------------+--------+-------------------+
|         Description|airlines|avg_departure_delay|
+--------------------+--------+-------------------+
|Southwest Airline...|   19393| 13.429567657134724|
|ExpressJet Airlin...|   20366| 12.296210112379818|
|United Air Lines ...|   19977|  8.818392620527979|
|Frontier Airlines...|   20436|  8.716275167785234|
| JetBlue Airways: B6|   20409|   8.31110357194785|
|       Envoy Air: MQ|   20398|  7.372135487994157|
|  Virgin America: VX|   21171|  6.989682212133719|
|American Airlines...|   19805|  6.733031255779545|
|SkyWest Airlines ...|   20304|   6.05227892794094|
|Delta Air Lines I...|   19790|  5.597661140117859|
|AirTran Airways C...|   20437|  5.110621095185594|
| US Airways Inc.: US|   20355| 3.9925874044242105|
|Alaska Airlines I...|   19930|-0.6991515343747522|
|Hawaiian Airlines...|   19690|-2.1981308411214955|
+--------------------+--------+-------------------+


Process finished with exit code 0
'''