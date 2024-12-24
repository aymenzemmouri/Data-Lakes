import sqlite3
import pymongo
from transformers import AutoTokenizer
from datetime import datetime

def process_and_insert_data(sqlite_db_path, mongo_uri, mongo_db_name, mongo_collection_name, model_name):
    """
    Processes data from a SQLite database and inserts it into a MongoDB collection.

    Steps:
    1. Connect to the SQLite database and fetch data from the 'texts' table.
    2. Initialize a MongoDB connection.
    3. Tokenize the text data using a pre-trained tokenizer.
    4. Insert processed documents into the MongoDB collection.

    Parameters:
    sqlite_db_path (str): Path to the SQLite database file.
    mongo_uri (str): URI for connecting to MongoDB.
    mongo_db_name (str): Name of the MongoDB database.
    mongo_collection_name (str): Name of the MongoDB collection.
    model_name (str): Name of the tokenizer model.
    """
    
    # Step 1: Connect to SQLite database
    print("Connecting to SQLite database...")
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    cursor = sqlite_conn.cursor()
    
    # Fetch all data from the 'texts' table
    cursor.execute("SELECT id, content AS text FROM texts")
    rows = cursor.fetchall()  # List of tuples (id, text)
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

# Main entry point with argument parsing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process data from SQLite to MongoDB")
    parser.add_argument("--sqlite_db_path", type=str, required=True, help="Path to the SQLite database file")
    parser.add_argument("--mongo_uri", type=str, required=True, help="MongoDB connection URI")
    parser.add_argument("--mongo_db_name", type=str, required=True, help="Name of the MongoDB database")
    parser.add_argument("--mongo_collection_name", type=str, required=True, help="Name of the MongoDB collection")
    parser.add_argument("--model_name", type=str, default="distilbert-base-uncased", help="Tokenizer model name")
    
    args = parser.parse_args()

    process_and_insert_data(
        sqlite_db_path=args.sqlite_db_path,
        mongo_uri=args.mongo_uri,
        mongo_db_name=args.mongo_db_name,
        mongo_collection_name=args.mongo_collection_name,
        model_name=args.model_name
    )