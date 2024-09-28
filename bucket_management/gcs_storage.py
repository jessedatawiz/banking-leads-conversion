import os
import yaml
from google.cloud import storage
from google.api_core.exceptions import Conflict


class GCPStorage:
    def __init__(self, project_id):
        self.client = storage.Client(project=project_id)
        
    def create_bucket(self, bucket_name):
        """Creates a new bucket in GCP if it doesn't exist."""
        try:
            # Check if the bucket already exists
            bucket = self.client.lookup_bucket(bucket_name)
            if bucket:
                print(f"Bucket {bucket_name} already exists.")
            else:
                bucket = self.client.create_bucket(bucket_name)
                print(f"Bucket {bucket.name} created.")
            return bucket
        except Conflict as e:
            print(f"Error: Bucket {bucket_name} already exists. {e}")
        except Exception as e:
            print(f"Error creating bucket: {e}")
            return None
    
    def upload_file(self, bucket_name, source_file_path, destination_blob_name):
        """Uploads a file to the bucket."""
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            # Check if the blob already exists
            if blob.exists():
                print(f"Blob {destination_blob_name} already exists in bucket {bucket_name}.")
            else:
                blob.upload_from_filename(source_file_path)
                print(f"File {source_file_path} uploaded to {destination_blob_name}.")
    
        except Exception as e:
            print(f"Error uploading file: {e}")

def load_config(file_path):
    with open(file_path, 'r') as stream:
        return yaml.safe_load(stream)

def upload_directory(gcp_storage, bucket_name, source_directory, destination_blob_prefix):
    """Recursively uploads files from a directory to the GCP bucket."""
    for dirpath, _, filenames in os.walk(source_directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            # Calculate relative path to maintain directory structure in the bucket
            relative_path = os.path.relpath(file_path, source_directory)
            destination_blob_name = os.path.join(destination_blob_prefix, relative_path)
            gcp_storage.upload_file(bucket_name, file_path, destination_blob_name)

# Usage example
if __name__ == "__main__":
    # Load configuration from the YAML file
    config = load_config('config.yaml')
    
    project_id = config['gcp']['project_id']
    bucket_name = config['gcp']['bucket_name']
    source_directory = config['gcp']['source_file_name']
    destination_blob_prefix = config['gcp']['destination_blob_name']
    
    # Initialize GCP storage facade
    gcp_storage = GCPStorage(project_id)

    # Create a bucket (commented out for now)
    gcp_storage.create_bucket(bucket_name)

    # Upload files from the directory
    upload_directory(gcp_storage, bucket_name, source_directory, destination_blob_prefix)
