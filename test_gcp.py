from google.cloud import storage

def test_gcp_connection():
  """
  Tests the connection to Google Cloud Platform (GCP) by attempting to 
  list buckets in your project.
  """
  try:
      # Instantiate a storage client
      storage_client = storage.Client()
      print(storage_client.project)

      # List buckets
      buckets = list(storage_client.list_buckets())

      if buckets:
          print("Connection successful!")
          print("Buckets in your project:")
          for bucket in buckets:
              print(bucket.name)
      if storage_client:  
          print("Connection successful, but no buckets found in your project.")

  except Exception as e:
      print(f"Connection failed: {e}")

if __name__ == "__main__":
  test_gcp_connection()