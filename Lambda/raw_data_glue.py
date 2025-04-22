import boto3
import urllib.request
from botocore.exceptions import ClientError
import json

def lambda_handler(event, context):
    """
    Lambda handler that fetches raw trip data and starts a Glue job.

    Args:
        event (dict): Lambda event data
        context (object): Lambda context

    Returns:
        dict: Response with status and details
    """
    # Extract parameters from the event or use defaults
    year = event.get('year', 2025)
    month = event.get('month', 1)
    bucket_name = event.get('bucket_name', 'jaath-buckets-0491f6b4-2be4-4ab9-9aa2-c62891ad4a9c')
    s3_key_prefix = event.get('s3_key_prefix', 'taxi/raw')
    glue_job_name = event.get('glue_job_name', 'iadFilterAndTransform')

    # Fetch the raw data
    fetch_result = fetch_raw_trip_data(year, month, bucket_name, s3_key_prefix)

    # Start the Glue job
    glue_result = start_glue_job(glue_job_name, year, month, bucket_name, s3_key_prefix)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'fetch_result': fetch_result,
            'glue_job': glue_result
        })
    }

def fetch_raw_trip_data(year: int, month: int, bucket_name: str, s3_key_prefix: str) -> str:
    """
    Fetch raw trip data for a given year and month, and upload it to an S3 bucket
    only if the file does not already exist.

    Args:
        year (int): The year of the trip data.
        month (int): The month of the trip data.
        bucket_name (str): The name of the S3 bucket.
        s3_key_prefix (str): The prefix (folder path) in the S3 bucket.

    Returns:
        str: The S3 key (path) where the file was uploaded or a message indicating it already exists.

    Raises:
        Exception: If the file cannot be downloaded or uploaded.
    """
    # Construct the URL for the trip data
    URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet"

    # Define the S3 key (path in the bucket)
    s3_key = f"{s3_key_prefix}/year={year}/month={month:02}/yellow_tripdata_{year}-{month:02}.parquet"

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Check if the file already exists in the S3 bucket
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"File already exists in S3: s3://{bucket_name}/{s3_key}")
        return f"File already exists in S3: s3://{bucket_name}/{s3_key}"
    except ClientError as e:
        # If the error is not a 404 (Not Found), re-raise it
        if e.response['Error']['Code'] != "404":
            raise Exception(f"Error checking file existence in S3: {e}")

    # Fetch the data from the URL
    print(f"Fetching raw data ...")
    try:
        with urllib.request.urlopen(URL) as response:
            if response.status == 200:
                print(f"Done fetching raw data ...")
                # File content
                file_content = response.read()

                try:
                    # Upload the file to S3
                    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)
                    print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")
                    return f"File uploaded to S3: s3://{bucket_name}/{s3_key}"
                except ClientError as e:
                    raise Exception(f"Failed to upload file to S3: {e}")
            else:
                raise Exception(f"{URL} returned status code {response.status}")
    except Exception as e:
        raise Exception(f"Failed to fetch data from {URL}: {e}")

def start_glue_job(job_name: str, year: int, month: int, bucket_name: str, s3_key_prefix: str) -> str:
    """
    Start a Glue job with the specified parameters.

    Args:
        job_name (str): The name of the Glue job to start
        year (int): The year parameter for the job
        month (int): The month parameter for the job
        bucket_name (str): The S3 bucket name parameter
        s3_key_prefix (str): The S3 key prefix parameter

    Returns:
        str: Job run ID or error message
    """
    glue_client = boto3.client('glue')

    try:
        # Start the Glue job with parameters
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--year': str(year),
                '--month': str(month),
                '--bucket_name': bucket_name,
                '--s3_key_prefix': s3_key_prefix
            }
        )

        job_run_id = response['JobRunId']
        print(f"Started Glue job {job_name} with run ID: {job_run_id}")
        return f"Started Glue job {job_name} with run ID: {job_run_id}"

    except Exception as e:
        error_msg = f"Failed to start Glue job {job_name}: {str(e)}"
        print(error_msg)
        return error_msg