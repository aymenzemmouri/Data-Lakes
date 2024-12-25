import os
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import sqlite3
import pymongo
from transformers import AutoTokenizer

# Define functions for each task
def extract_data(**kwargs):
    input_dir = "data/raw"
    bucket_name = "raw"
    output_file_name = "combined_raw.txt"

    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    text = ""

    # Iterate through train, test, and dev subfolders
    for subfolder in ['train', 'test', 'dev']:
        subfolder_path = os.path.join(input_dir, subfolder)
        if os.path.exists(subfolder_path) and os.path.isdir(subfolder_path):
            for file_name in os.listdir(subfolder_path):
                file_path = os.path.join(subfolder_path, file_name)
                print(f"Reading {file_path}")
                
                # Read the text file and append to text
                with open(file_path, 'r') as file:
                    text += file.read()
        else:
            print(f"Subfolder {subfolder_path} does not exist or is not a directory.")

    # Combine all data into a single string and upload to S3
    if text:
        # Save the combined data to a local file before uploading
        text_path = f"/tmp/{output_file_name}"  
        with open(text_path, 'w') as file:
            file.write(text)

        print(f"Combined file saved locally at {text_path}.")

        # Upload the combined file to the S3 bucket
        with open(text_path, "rb") as file:
            s3.upload_fileobj(file, bucket_name, output_file_name)

        print(f"Uploaded combined file to bucket '{bucket_name}' with name '{output_file_name}'.")
    else:
        print("No valid files found to process.")

def transform_data(**kwargs):
    bucket_raw = "raw"
    input_file = "combined_raw.txt"
    db_host = "localhost"

    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    # Step 1: Download raw data
    response = s3.get_object(Bucket=bucket_raw, Key=input_file)
    data = response['Body'].read().decode('utf-8')

    # Step 2: Handle missing values
    print("Cleaning data by removing missing values...")
    lines = data.splitlines()
    cleaned_lines = list(dict.fromkeys(line.strip() for line in lines if line.strip()))
    cleaned_data = "\n".join(cleaned_lines)

    # Split the text into chapters based on the pattern
    pattern = r"^=([^=]+)=$"
    chapter_titles = re.findall(pattern, cleaned_data, re.MULTILINE)

    chapters = {}
    lines = cleaned_data.split("\n")
    current_chapter_to_treat = 0
    start_index = 0
    end_index = 0

    # Loop through each line and split the text into chapters
    for i, line in enumerate(lines):
        if current_chapter_to_treat < len(chapter_titles) and chapter_titles[current_chapter_to_treat] in line:
            start_index = i
        if (current_chapter_to_treat + 1 < len(chapter_titles) 
                and chapter_titles[current_chapter_to_treat + 1] in line):
            end_index = i
            chapters[chapter_titles[current_chapter_to_treat]] = lines[start_index:end_index]
            current_chapter_to_treat += 1

    if current_chapter_to_treat < len(chapter_titles):
        chapters[chapter_titles[current_chapter_to_treat]] = lines[start_index:]

    # Step 3: Connect to SQLite and insert data
    print(f"Connecting to the SQLite database on {db_host}...")
    conn = sqlite3.connect('/opt/airflow/data.db')  # Ensure correct path for DB
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS texts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            content TEXT
        )
    """)

    try:
        for title, content in chapters.items():
            cursor.execute('''
                INSERT INTO texts (title, content) 
                VALUES (?, ?)
            ''', (title, "\n".join(content)))
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Database connection closed.")

def load_data(**kwargs):
    sqlite_db_path = "/opt/airflow/data.db"  # Ensure correct path
    mongo_uri = "mongodb://localhost:27017/"
    mongo_db_name = "curated"
    mongo_collection_name = "wikitext"
    model_name = "distilbert-base-uncased"

    # Step 1: Connect to SQLite database
    print("Connecting to SQLite database...")
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    cursor = sqlite_conn.cursor()
    
    # Fetch all data from the 'texts' table
    cursor.execute("SELECT id, content AS text FROM texts")
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} rows from SQLite database.")
    
    # Step 2: Initialize MongoDB connection
    print("Connecting to MongoDB...")
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]
    
    # Step 3: Initialize the tokenizer
    print("Initializing tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    # Step 4: Process rows and insert into MongoDB
    print("Processing rows and inserting into MongoDB...")
    for row in rows:
        row_id, text = row
        # Tokenize the text
        tokens = tokenizer(text, truncation=True, padding=True, max_length=128)["input_ids"]
        
        # Create the document to insert into MongoDB
        document = {
            "id": row_id,
            "text": text,
            "tokens": tokens,
            "metadata": {
                "source": "sqlite",
                "processed_at": datetime.utcnow().isoformat()
            }
        }
        
        # Insert the document into MongoDB
        mongo_collection.insert_one(document)
    
    print(f"Data successfully inserted into MongoDB collection '{mongo_collection.name}'.")
    
    # Step 5: Close connections
    print("Closing database connections...")
    cursor.close()
    sqlite_conn.close()
    mongo_client.close()
    print("All connections closed.")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Adjust the start date as needed
    'retries': 1,
}

# Define the DAG
with DAG(
    'data_lake_pipeline',  # Name of the DAG
    default_args=default_args,
    schedule_interval=None  # Adjust schedule as needed
) as dag:

    # Define tasks using PythonOperator
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task