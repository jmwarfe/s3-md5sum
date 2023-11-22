import boto3
import hashlib
import csv
import sys
import io
import urllib.parse
from prefect import flow, task, get_run_logger

from prefect.task_runners import ConcurrentTaskRunner


def calculate_md5sum(bucket_name, object_key):
    s3_client = boto3.client('s3')

    try:
        # Get the object from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        obj_body = obj['Body']
    
        # Initialize MD5 hash object
        md5_hash = hashlib.md5()

        # Read the object in chunks and update the MD5 hash
        for chunk in iter(lambda: obj_body.read(1024 * 1024), b''):
            md5_hash.update(chunk)

        # Return the MD5 checksum as a hex string
        return md5_hash.hexdigest()
    except Exception as e:
        print("Exception Occurred while hashing MD5: ",e)
        return()

@task
def check_file_hash(s3_uri, manifest_checksum):
    
    # Parse the S3 URI
    parsed_uri = urllib.parse.urlparse(s3_uri)
    s3_file = parsed_uri.path
    s3_bucket = parsed_uri.netloc

    # Calculate S3 object md5sum
    s3_md5sum = calculate_md5sum(s3_bucket, s3_file.strip('/') )

    # Compare the calculated checksum to the manifest checksum
    if s3_md5sum == manifest_checksum:
        print(f"File {s3_uri} matches the manifest.")
    else:
        print(f"File {s3_uri} does not match the manifest.")

@flow(task_runner=ConcurrentTaskRunner, name="Check MD5 of S3 URI Against Manifest",log_prints=True )

def runner(bucket_name, manifest_file):
    s3_client = boto3.client('s3')
    
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=manifest_file)
        file_content = response['Body'].read().decode('utf-8')
        csv_file = io.StringIO(file_content)
    except Exception as e:
        print("Exception Occurred while Reading Manifest: ",e)

    reader = csv.reader(csv_file, delimiter='\t')
    for row in reader:
        s3_uri = row[4]
        md5sum = row[1]
        check_file_hash.submit(s3_uri, md5sum)

 
if __name__ == "__main__":
    bucket_name=''
    manifest_file=''
    runner( bucket_name, manifest_file)