import mysql.connector
import pandas as pd
from google.cloud import storage
import datetime
import os
import dotenv

dotenv.load_dotenv()

# config
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')

# Ensure environment variables are set
if not all([DB_HOST, DB_USER, DB_NAME, GCS_BUCKET_NAME]):
    print("Error: One or more environment variables are not set.")
    exit(1)

# tables must have a 'updated_at' column.
TABLES = ['users', 'discounts', 'products', 'orders', 'order_details', 'payouts', 'payout_payments', 'payout_resellers']

# the absolute path 
script_dir = os.path.dirname(os.path.abspath(__file__))

# paths relative to the script's dir
TIMESTAMP_DIR = os.path.join(script_dir, 'timestamps')
EXPORT_DIR = os.path.join(script_dir, 'exports')

# make sure dirs exist
os.makedirs(TIMESTAMP_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

def get_last_timestamp(table_name):
    """Reads the last export timestamp for a given table from a file."""
    timestamp_file = os.path.join(TIMESTAMP_DIR, f"{table_name}_last_run.txt")
    if not os.path.exists(timestamp_file):
        return datetime.datetime.min
    with open(timestamp_file, 'r') as f:
        timestamp_str = f.read().strip()
        if timestamp_str:
            return datetime.datetime.fromisoformat(timestamp_str)
    return datetime.datetime.min

def save_current_timestamp(table_name, timestamp):
    """Saves the current timestamp for a given table to a file."""
    timestamp_file = os.path.join(TIMESTAMP_DIR, f"{table_name}_last_run.txt")
    with open(timestamp_file, 'w') as f:
        f.write(timestamp.isoformat())

def main():
    """Main function to perform the incremental backup and upload."""
    
    # Authenticate to Google Cloud (using Workload Identity Federation or a key file)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
    except Exception as e:
        print(f"Error authenticating to Google Cloud: {e}")
        return

    # Connect to MySQL
    try:
        cnx = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return

    # Iterate over each table
    for table_name in TABLES:
        try:
            print(f"--- Processing table: {table_name} ---")
            last_timestamp = get_last_timestamp(table_name)
            
            cursor = cnx.cursor(dictionary=True)
            
            # incremental backup strategy
            query = f"SELECT * FROM {table_name} WHERE updated_at > %s ORDER BY updated_at ASC"
            cursor.execute(query, (last_timestamp,))
            
            rows = cursor.fetchall()
            
            if not rows:
                print("No new data to export.")
                cursor.close()
                continue
                
            df = pd.DataFrame(rows)
            
            # Create a file name with a unique timestamp
            current_time = datetime.datetime.now()
            file_name = f"{table_name}_{current_time.strftime('%Y-%m-%d_%H%M%S')}.csv"
            file_path = os.path.join(EXPORT_DIR, file_name)
            df.to_csv(file_path, index=False)
            
            print(f"Exported {len(rows)} rows to {file_path}")
            
            # Upload to GCS
            destination_blob_name = f"{table_name}/{file_name}"
            # upload the file to GCS.
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(file_path)
            
            print(f"Uploaded to GCS bucket: gs://{GCS_BUCKET_NAME}/{destination_blob_name}")
            
            # Update the high water mark
            new_high_water_mark = df['updated_at'].max()
            save_current_timestamp(table_name, new_high_water_mark)
            
            cursor.close()
            
            # clean after uploading
            os.remove(file_path) # Clean up the local file

        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            # Add robust error logging here

    cnx.close()
    print("All tables processed. Database connection closed.")

if __name__ == "__main__":
    main()
