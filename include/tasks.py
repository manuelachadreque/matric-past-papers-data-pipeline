import requests
from airflow.hooks.base import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.base import PokeReturnValue


links = {
    "Mathematics": "https://s3.af-south-1.amazonaws.com/www.teachme2.com/matric-past-papers/2018-2023-Mathematics.zip",
    "Physical Sciences": "https://s3.af-south-1.amazonaws.com/www.teachme2.com/matric-past-papers/2018-2023-Physical-Sciences.zip",
    "Accounting": "https://s3.af-south-1.amazonaws.com/www.teachme2.com/matric-past-papers/2018-2023-Accounting.zip",
    "English": "https://s3.af-south-1.amazonaws.com/www.teachme2.com/matric-past-papers/2018-2023-English.zip"
}


# Check if a link is available
def check_link_availability(subject, url, **kwargs):
    try:
        response = requests.head(url, allow_redirects=True)  # Use HEAD for a lightweight check
        if response.status_code == 200:
            print(f"Link {url} for {subject} is available.")
            return {"subject": subject, "url": url, "status": "available"}
        else:
            print(f"Link {url} for {subject} is not available. Status code: {response.status_code}")
            return {"subject": subject, "url": url, "status": "unavailable"}
    except Exception as e:
        print(f"Failed to connect to {url} for {subject}: {e}")
        return {"subject": subject, "url": url, "status": "unavailable"}
        
# Task to check all links
def verify_links(**kwargs):
    ti = kwargs['ti']
    for subject, url in links.items():
        # Pass both subject and URL to the check function
        availability = check_link_availability(subject, url)
        ti.xcom_push(key=f"{subject}_availability", value=availability)



def upload_files_to_s3(**Kwargs):
    bucket_name =Kwargs['bucket_name']
    aws_conn_id =Kwargs['aws_connection']



    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    for subject, url in links.items():
        try:
            response = requests.get(url, stream= True)
            response.raise_for_status()


            s3_key =f"Landing/{subject}/{subject}.zip"

            s3_hook.load_bytes(
                bytes_data =response.content,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )

        except Exception as e:
            print(f"Error while loading file {subject}: {e}")

