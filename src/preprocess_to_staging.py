import io
import sqlite3
import pandas as pd
import boto3
import numpy as np
import tqdm
import joblib
from collections import OrderedDict
from sklearn.preprocessing import LabelEncoder
from numba import njit
import re

def preprocess_to_staging(bucket_raw, db_host, db_user, db_password, input_file):
    """
    Preprocesses data from the raw bucket and uploads preprocessed data splits to the staging bucket.

    Steps:
    •	Download the data from the raw bucket.
	•	Clean the data (remove duplicates and empty rows).
	•	Connect to the SQLite database.
	•	Create a table texts (if it does not exist) with the necessary columns.
	•	Insert the cleaned data into this table.
	•	Verify that the data has been correctly inserted.

    Parameters:
    bucket_raw (str): Name of the raw S3 bucket.
    db_host (str): Database host (used only for logging).
    db_user (str): Database user (ignored for SQLite).
    db_password (str): Database password (ignored for SQLite).
    input_file (str): Name of the input file in the raw bucket.
    """
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    # Step 1: Download raw data
    response = s3.get_object(Bucket=bucket_raw, Key=input_file)
    data = response['Body'].read().decode('utf-8')

    # Step 2: Handle missing values
    print("Cleaning data by removing missing values...")
    
    # Remove duplicates and empty rows
    lines = data.splitlines()
    cleaned_lines = list(dict.fromkeys(line.strip() for line in lines if line.strip()))
    cleaned_data = "\n".join(cleaned_lines)
    
    # The text is composed of multiple lines, with each chapter represented by = Chapter =, and the second chapter starting with == Chapter == etc 
    # Split the text into chapters
    pattern = r"^=([^=]+)=$"

    # Find all matches (chapter titles)
    chapter_titles = re.findall(pattern, cleaned_data, re.MULTILINE)

    # Dictionary to store the line numbers and content for each chapter
    chapters = {}

    # Split the cleaned data into lines once for reusability
    lines = cleaned_data.split("\n")

    # Index to track the current chapter
    current_chapter_to_treat = 0

    start_index = 0
    end_index = 0

    # Loop through each line in cleaned_data
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

    # Step 3: Connect to the SQLite database
    print(f"Connecting to the SQLite database on {db_host}...")
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()

    # Create the table "texts" if it does not exist
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess data from raw to staging bucket")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Name of the raw S3 bucket")
    parser.add_argument("--db_host", type=str, required=False, default="localhost", help="Database host (default: localhost)")
    parser.add_argument("--db_user", type=str, required=False, help="Database user (ignored for SQLite)")
    parser.add_argument("--db_password", type=str, required=False, help="Database password (ignored for SQLite)")
    parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in the raw bucket")
    args = parser.parse_args()

    preprocess_to_staging(
        bucket_raw=args.bucket_raw,
        db_host=args.db_host,
        db_user=args.db_user,
        db_password=args.db_password,
        input_file=args.input_file,
    )