from sanic import Sanic, response
from datetime import datetime
import boto3
import pymysql
from pymongo import MongoClient

app = Sanic("TP6API")

# Configurations pour LocalStack
S3_BUCKET_NAME = "raw"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
LOCALSTACK_ENDPOINT = "http://localhost:4566"

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,  
    "user": "root",
    "password": "root",
    "database": "staging",
}

# MongoDB configuration dictionary
MONGODB_CONFIG = {
    "uri": "mongodb://localhost:27017",  
    "database": "curated",              
    "collection": "wikitext"   
}

# Fonction pour vérifier la connexion S3 via LocalStack
def check_s3_connection():
    try:
        s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=LOCALSTACK_ENDPOINT,  # Important pour pointer vers LocalStack
        )
        s3.head_bucket(Bucket=S3_BUCKET_NAME)
        return "connected"
    except Exception as e:
        return f"error: {str(e)}"

# Fonction pour vérifier la connexion MySQL
def check_mysql_connection():
    try:
        connection = pymysql.connect(
            host=MYSQL_CONFIG["host"],
            port=MYSQL_CONFIG["port"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
            database=MYSQL_CONFIG["database"],
        )
        connection.close()
        return "connected"
    except Exception as e:
        return f"error: {str(e)}"

# Fonction pour vérifier la connexion MongoDB
def check_mongo_connection():
    try:
        client = MongoClient(MONGODB_CONFIG["uri"])
        client.server_info()  # Lève une exception si la connexion échoue
        return "connected"
    except Exception as e:
        return f"error: {str(e)}"


# Fonction pour récupérer les fichiers dans le bucket S3
def list_s3_objects(limit=None):
    try:
        s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=LOCALSTACK_ENDPOINT, 
        )
        # Lister les objets dans le bucket
        objects = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if "Contents" not in objects:
            return []
        
        files = objects["Contents"]
        
        # Limiter le nombre d'éléments si un paramètre `limit` est défini
        if limit:
            files = files[:limit]

        return [{"Key": obj["Key"], "LastModified": obj["LastModified"].isoformat()} for obj in files]

    except Exception as e:
        return f"error: {str(e)}"

@app.get("/raw")
async def get_raw_data(request):
    # Récupération des paramètres de requête
    limit = request.args.get("limit", None)
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return response.json({"error": "Invalid limit parameter"}, status=400)

    # Récupérer les données S3
    data = list_s3_objects(limit=limit)

    # Si une erreur se produit dans `list_s3_objects`
    if isinstance(data, str) and data.startswith("error:"):
        return response.json({"error": data}, status=500)

    # Réponse JSON
    return response.json({
        "API Status": "online",
        "Timestamp": datetime.utcnow().isoformat(),
        "Raw Data": data,
    })


# Fonction pour exécuter une requête SQL
def execute_query(sql_query, params=None):
    try:
        connection = pymysql.connect(
            host=MYSQL_CONFIG["host"],
            port=MYSQL_CONFIG["port"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
            database=MYSQL_CONFIG["database"],
            cursorclass=pymysql.cursors.DictCursor,
        )
        with connection.cursor() as cursor:
            cursor.execute(sql_query, params)
            results = cursor.fetchall()
        connection.close()
        return results
    except Exception as e:
        return f"error: {str(e)}"

def serialize_data(data):
    """
    Recursively convert datetime objects in a dictionary or list to strings.
    """
    if isinstance(data, dict):
        return {k: serialize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

@app.get("/staging/")
async def get_staging_data(request):
    # Récupération des paramètres de requête
    query = request.args.get("query", None)
    
    print("query", query)
    if not query:
        return response.json({"error": "Query parameter is required"}, status=400)
    
    # Exécuter la requête SQL
    data = execute_query(query)

    # Si une erreur se produit dans `execute_query`
    if isinstance(data, str) and data.startswith("error:"):
        return response.json({"error": data}, status=500)

    serialized_data = serialize_data(data)

    return response.json({
        "API Status": "online",
        "Timestamp": datetime.utcnow().isoformat(),
        "Staging Data": serialized_data,
    })


def get_mongo_collection():
    try:
        client = MongoClient(MONGODB_CONFIG["uri"])
        db = client[MONGODB_CONFIG["database"]]
        return db[MONGODB_CONFIG["collection"]]
    except Exception as e:
        return f"error: {str(e)}"
    

@app.get("/curated/")
async def get_curated_data(request):
    """
    Endpoint to fetch documents from MongoDB with an optional limit parameter.
    """
    # Get query parameters
    limit = request.args.get("limit", None)

    try:
        # Connect to the MongoDB collection
        collection = get_mongo_collection()
        if isinstance(collection, str) and collection.startswith("error:"):
            return response.json({"error": collection}, status=500)

        # Fetch documents, applying limit if specified
        cursor = collection.find()
        if limit:
            try:
                limit = int(limit)
                cursor = cursor.limit(limit)
            except ValueError:
                return response.json({"error": "Invalid limit parameter"}, status=400)

        # Convert cursor to a list of dictionaries
        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])  # Serialize ObjectId
            results.append(doc['text'][:500])

        return response.json({
            "API Status": "online",
            "Timestamp": datetime.utcnow().isoformat(),
            "Curated Data": results,
        })

    except Exception as e:
        return response.json({"error": f"An error occurred: {str(e)}"}, status=500)
    
@app.get("/health")
async def health_check(request):
    # Vérifications des services
    s3_status = check_s3_connection()
    mysql_status = check_mysql_connection()
    mongodb_status = check_mongo_connection()

    # Réponse JSON
    health_status = {
        "API Status": "online",
        "Timestamp": datetime.utcnow().isoformat(),
        "Connections Status": {
            "S3": s3_status,
            "MySQL": mysql_status,
            "MongoDB": mongodb_status,
        },
    }
    return response.json(health_status)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)