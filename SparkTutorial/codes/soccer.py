from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions
"""
Find the best stricker in 2016
"""

sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession.builder.appName('Socccer Data analysis').getOrCreate()

# read player and attributes data
player = spark.read.csv(path='../data/player.csv', header=True)
player_attr = spark.read.csv(path='../data/player_attributes.csv', header=True)

print(player.printSchema())
'''
root
 |-- id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)
'''
print(player_attr.printSchema())
'''
root
 |-- id: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- overall_rating: string (nullable = true)
 |-- potential: string (nullable = true)
 |-- preferred_foot: string (nullable = true)
 |-- attacking_work_rate: string (nullable = true)
 |-- defensive_work_rate: string (nullable = true)
 |-- crossing: string (nullable = true)
 |-- finishing: string (nullable = true)
 |-- heading_accuracy: string (nullable = true)
 |-- short_passing: string (nullable = true)
 |-- volleys: string (nullable = true)
 |-- dribbling: string (nullable = true)
 |-- curve: string (nullable = true)
 |-- free_kick_accuracy: string (nullable = true)
 |-- long_passing: string (nullable = true)
 |-- ball_control: string (nullable = true)
 |-- acceleration: string (nullable = true)
 |-- sprint_speed: string (nullable = true)
 |-- agility: string (nullable = true)
 |-- reactions: string (nullable = true)
 |-- balance: string (nullable = true)
 |-- shot_power: string (nullable = true)
 |-- jumping: string (nullable = true)
 |-- stamina: string (nullable = true)
 |-- strength: string (nullable = true)
 |-- long_shots: string (nullable = true)
 |-- aggression: string (nullable = true)
 |-- interceptions: string (nullable = true)
 |-- positioning: string (nullable = true)
 |-- vision: string (nullable = true)
 |-- penalties: string (nullable = true)
 |-- marking: string (nullable = true)
 |-- standing_tackle: string (nullable = true)
 |-- sliding_tackle: string (nullable = true)
 |-- gk_diving: string (nullable = true)
 |-- gk_handling: string (nullable = true)
 |-- gk_kicking: string (nullable = true)
 |-- gk_positioning: string (nullable = true)
 |-- gk_reflexes: string (nullable = true)
'''

# make dataframe kight by selecting required attributes
player_attr = player_attr.select('player_api_id','date','shot_power','finishing','acceleration')

# udf function to retrive the year from date
extract_year_udf = udf(lambda date : date.split('-')[0])

# add calculated column year
player_attr = player_attr.withColumn(
    "year",
    extract_year_udf(player_attr.date))

# drop date column as it has no further use
player_attr = player_attr.drop("date")
#player_attr.show(5)

# filter player attributes for latest year which is 2016
player_attr = player_attr.filter(player_attr.year == 2016)

# calculate average
player_attr = player_attr.groupBy('player_api_id')\
    .agg({"shot_power":"avg", "finishing":"avg","acceleration":"avg"})

# rename column
player_attr = player_attr\
    .withColumnRenamed("avg(finishing)","finishing")\
    .withColumnRenamed("avg(acceleration)",'acceleration')\
    .withColumnRenamed("avg(shot_power)", 'shot_power')


finishing_weight = 2
acceleration_weight = 1
shot_power_weight = 1

total_weight = finishing_weight \
               + acceleration_weight \
               + shot_power_weight

# add weaight to each attributes
player_attr = player_attr.withColumn("weighted_score",
                                     (player_attr.finishing * finishing_weight +
                                     player_attr.acceleration *  acceleration_weight +
                                     player_attr.shot_power * shot_power_weight )/total_weight)

player_attr = player_attr.drop("finishing","acceleration","shot_power")
top_ten_stricker = player_attr.orderBy("weighted_score", ascending=False).limit(10)

# full detail about these 10 stickers
selected_strickers = top_ten_stricker.join(player, ["player_api_id"])

selected_strickers.show()
'''
|player_api_id|   weighted_score|  id|         player_name|player_fifa_api_id|           birthday|height|weight|
+-------------+-----------------+----+--------------------+------------------+-------------------+------+------+
|       169193|           85.375| 482| Alexandre Lacazette|            193301|1991-05-28 00:00:00|175.26|   161|
|        30834|             86.5| 951|        Arjen Robben|              9014|1984-01-23 00:00:00|180.34|   176|
|        38817|           88.625|1581|        Carlos Tevez|            143001|1984-02-05 00:00:00|172.72|   157|
|        31921|             85.5|3660|         Gareth Bale|            173731|1989-07-16 00:00:00|182.88|   163|
|        25759|85.83333333333334|3936|     Gonzalo Higuain|            167664|1987-12-10 00:00:00|182.88|   181|
|        20276|             87.0|4283|                Hulk|            189362|1986-07-25 00:00:00|180.34|   187|
|        32118|            86.25|6400|      Lukas Podolski|            150516|1985-06-04 00:00:00|182.88|   183|
|        19533|           86.375|7867|              Neymar|            190871|1992-02-05 00:00:00|175.26|   150|
|       150565|            87.25|8625|Pierre-Emerick Au...|            188567|1989-06-18 00:00:00|187.96|   176|
|        37412|            89.75|9674|       Sergio Aguero|            153079|1988-06-02 00:00:00|172.72|   163|
+-------------+-----------------+----+--------------------+------------------+-------------------+------+------+
'''