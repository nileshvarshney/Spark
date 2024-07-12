from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage, bigquery
from google.cloud.bigquery import table



PROJECT = "<PROJECT_ID>"
GCS_BUCKET = "<GCS_BUCKET>"

def create_bq_normal_table(bq_project, bq_dataset, bq_table, source_data_loc):
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.use_avro_logical_types=True

    table_ref = bq_client.dataset(bq_dataset, project=bq_project).table(bq_table)
    uri = source_data_loc + '*.parquet'
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)

    print("Job starting {}".format(load_job.job_id))
    destination_table = bq_client.get_table(table_ref)
    print("Loaded {} rows".format(destination_table.num_rows))
    print("Loading Job Finished")


# Main
# create spark session
spark = SparkSession.builder.appName('flightPipeLine').getOrCreate()

airlines = 'gs://{}/data/airlines.csv'.format(GCS_BUCKET)
flights = 'gs://{}/data/airlines.csv'.format(GCS_BUCKET)
airports = 'gs://{}/data/airports.csv'.format(GCS_BUCKET)

airlines_df = spark.read.csv(airlines, header=True)
print(airlines_df.printSchema())
print(airlines_df.show(5))

flights_df = spark.read.csv(flights, header=True)
print(flights_df.printSchema())
print(flights_df.show(5))

airports_df = spark.read.csv(airports, header=True)
print(airports_df.printSchema())
print(airports_df.show(5))

delayed_flights = (
    flights.filter(col("departure_delay" > 0))
    .select("date", "airlines", "flight_number", "origin", "destination")
)

delayed_flights = (
    delayed_flights.join(
        airlines_df,
        airlines_df.Code == delayed_flights.airlines, 
        'left')
        .drop("Code")
        .withColumnRenamed("Description", "airline_name")
    )

delayed_flights = (
    delayed_flights.join(
        airports_df,
        airports_df.Code == delayed_flights.destination, 
        'left')
        .drop("Code")
        .withColumnRenamed("Description", "airport_name")
    )

print(delayed_flights.printSchema())
print(delayed_flights.show(10))

# Save flight data to GCS
v_target_bucket = "gs://{}/output/{}/".format(GCS_BUCKET,"flights")
flights_df.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","flights", v_target_bucket) 

v_target_bucket = "gs://{}/{}/".format(GCS_BUCKET,"airlines")
airlines_df.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","airlines", v_target_bucket) 

v_target_bucket = "gs://{}/{}/".format(GCS_BUCKET,"delayed_flights")
delayed_flights.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","delayed_flights", v_target_bucket) 

v_target_bucket = "gs://{}/{}/".format(GCS_BUCKET,"airports")
airports_df.write.mode("overwrite").format("parquet").save(v_target_bucket)
create_bq_normal_table(PROJECT,"airlines_db","airports", v_target_bucket) 

