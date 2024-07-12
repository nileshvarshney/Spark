-- Cluster Provision
gcloud services enable dataproc.googleapis.com
gsutil mb -l us-central1 gs://$DEVSHELL_PROJECT_ID-dataproc
gcloud dataproc clusters create blueseas --region=us-central1 --zone=us-central1-f --single-node --master-machine-type=n1-standard-2

-- run flight_airlines_dataload
gcloud dataproc jobs submit pyspark flights_pipeline.py --cluster=blueseas --region=us-central1

-- run soccer
gcloud dataproc jobs submit pyspark soccer.py --cluster=blueseas --region=us-central1

-- run products
gcloud dataproc jobs submit pyspark products.py  --cluster=blueseas --region=us-central1