import boto3
import os


# Name of the Glue job as created in AWS Console
GLUE_JOB_NAME = os.getenv("CURATE_GLUE_JOB_NAME", "curate_catalog")


def main():
    """
    Trigger the AWS Glue job that runs curate_catalog ETL.
    This function is called by Airflow (PythonOperator).
    """
    glue = boto3.client("glue", region_name="us-east-2")  # change if your region is different

    print(f"Starting Glue job: {GLUE_JOB_NAME}")
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)

    job_run_id = response["JobRunId"]
    print(f"Glue job started. JobRunId = {job_run_id}")
