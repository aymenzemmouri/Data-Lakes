from elasticsearch import Elasticsearch
import json
import argparse
import boto3
from io import BytesIO

class ElasticsearchHandler:
    def __init__(self, host="localhost", port=9200):
        self.es = Elasticsearch([{'host': host, 'port': port}])
        
    def create_index(self, index_name="hackernews"):
        # Créez une fonction qui génère un index elasticsearch qui correspondra aux stories hackernews
        # L'index devrait avoir les champs suivants :
        #            "id": {"type": "integer"},
        #            "title": {"type": "text"},
        #            "url": {"type": "keyword"},
        #            "score": {"type": "integer"},
        #            "timestamp": {"type": "date"}
    
    def index_stories(self, stories, index_name="hackernews"):
        # Créez une fonction qui indexe les stories que nous avons récupérées sur l'index 'hackernews'
        # En vocabulaire Elasticsearch, vous pouvez le comprendre comme 'ajouter à une table'

def get_stories_from_s3(endpoint_url):
    """Récupère les stories depuis S3."""
    s3_client = boto3.client('s3', endpoint_url=endpoint_url)
    
    # Ecrivez le code permettant de récupérer les stories brutes stockées sur le bucket raw pour 
    # les injecter dans ES

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