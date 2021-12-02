## Apache Beam Dataflow

### Details
This project accepts an input csv file and uses dataframe within cloud datalow and outputs the original and final CSV(after group By transformation) to BigQuery dataset.


### Command to Run in cloud shell or local

```
export GOOGLE_APPLICATION_CREDENTIALS=<<path_to_service_account_json>>
export PROJECT=<<gcp_project_id>>
export BUCKET=<<gcs_bucket_name>>
export REGION=us-east1

gcloud config set project $PROJECT
python tranform.py  --region $REGION  --runner DataflowRunner  --project $PROJECT --temp_location gs://$BUCKET/tmp/ --input gs://$BUCKET/AB_NYC_2019.csv \
--template_location gs://$BUCKET/templates/conversion_gcs_bq

 gcloud dataflow jobs run airbnb-2019-sweta-df-job12 --gcs-location gs://airbnb-ny-2019-sweta/templates/conversion_gcs_bq_16 --region us-east1
 ```