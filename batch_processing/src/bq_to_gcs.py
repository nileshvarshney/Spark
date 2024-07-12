from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage, bigquery
from google.cloud.bigquery import table



PROJECT = "<PROJECT_ID>"
GCS_BUCKET = "<GCS_BUCKET>"

def create_query_catch(v_query_txt):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.use_query_cache=False
    query_job = client.query(query=v_query_txt, job_config=job_config)
    query_job.result()
    query_cache = query_job.de.project +'.'+query_job.destination.dataset_id +'.' + query_job.destination.table_id
    print('Query Catch ID: {}'.format(query_cache))
    return {"query_cache_id": query_cache, "processed_bytes" : query_job.total_bytes_processed}


# Main
# create spark session
spark = SparkSession.builder.appName('bq_to_gcs').getOrCreate()

query = """ SELECT * FROM `{}.airlines_db.flights`""".format(PROJECT)
query_cach = create_query_catch(query)
df_flights = spark.read.format("bigquery").option('table',query_cach['query_cache_id']).load()

print(df_flights.printSchema())
print(df_flights.show(5, truncate=False))

spark.stop()
