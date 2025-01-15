from elasticsearch import Elasticsearch
import json
import argparse
import boto3
from io import BytesIO
import requests
import json

class ElasticsearchHandler:
    def __init__(self, host="localhost", port=9200, scheme="http"):
        self.es = Elasticsearch([{"host": host, "port": port, "scheme": scheme}],
                                headers={"Content-Type": "application/json"})
        
    def create_index(self, index_name="hackernews"):
        # Créez une fonction qui génère un index elasticsearch qui correspondra aux stories hackernews
        # L'index devrait avoir les champs suivants :
        #            "id": {"type": "integer"},
        #            "title": {"type": "text"},
        #            "url": {"type": "keyword"},
        #            "score": {"type": "integer"},
        #            "timestamp": {"type": "date"}

        # URL d'Elasticsearch
        url = f"http://localhost:9200/{index_name}"

        # En-têtes HTTP
        headers = {
            "Content-Type": "application/json"
        }

        # Corps de la requête (JSON Mapping)
        data = {
            "mappings": {
                "properties": {
                    "id": {"type": "integer"},
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "url": {"type": "keyword"},
                    "score": {"type": "integer"},
                    "timestamp": {"type": "date"}
                }
            }
        }

        # Exécution de la requête PUT
        response = requests.put(url, headers=headers, data=json.dumps(data))

        # Affichage de la réponse
        if response.status_code == 200 or response.status_code == 201:
            print("Index 'hackernews' créé avec succès !")
        else:
            print(f"Erreur {response.status_code}: {response.text}")
    
    def index_stories(self, stories, index_name="hackernews"):
        # Créez une fonction qui indexe les stories que nous avons récupérées sur l'index 'hackernews'
        # En vocabulaire Elasticsearch, vous pouvez le comprendre comme 'ajouter à une table'

        # Parcours des stories
        for story in stories:
            # Indexation de la story
            document = {
                "id": story["id"],
                "title": story["title"],
                "content": story["text"],
                "url": story["url"],
                "score": story["score"],
                "timestamp": story["timestamp"]
            }
            self.es.index(index=index_name, body=document)

        print(f"{len(stories)} stories indexées avec succès !")

def get_stories_from_s3(endpoint_url):
    """Récupère les stories depuis S3."""
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)

    # Ecrivez le code permettant de récupérer les stories brutes stockées sur le bucket raw pour 
    # les injecter dans ES

    # Récupération de la liste des objets
    response = s3_client.list_objects(Bucket='raw')

    stories = []
    # Récupération des objets

    for obj in response.get('Contents', []):
        key = obj['Key']
        response = s3_client.get_object(Bucket='raw', Key=key)
        body = response['Body'].read().decode('utf-8')
        story = json.loads(body)

        # Ajout de la story au dictionnaire 
        stories.append(story)
        print(story)


    return stories

    
    

def main():
    parser = argparse.ArgumentParser(description='Index stories to Elasticsearch')
    parser.add_argument('--host', type=str, default='elasticsearch',
                      help='Elasticsearch host')
    parser.add_argument('--port', type=int, default=9200,
                      help='Elasticsearch port')
    parser.add_argument('--index', type=str, default='hackernews',
                      help='Elasticsearch index name')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                      help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    # Récupération des stories depuis S3
    stories = get_stories_from_s3(args.endpoint_url)
    
    # Indexation dans Elasticsearch
    es_handler = ElasticsearchHandler(host=args.host, port=args.port)
    es_handler.index_stories(stories, args.index)

if __name__ == "__main__":
    main()