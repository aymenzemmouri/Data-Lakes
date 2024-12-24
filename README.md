# Data Lakes & Data Integration 

This repository is designed to help students learn about data lakes and data integration pipelines using Python, Docker, LocalStack, and DVC. Follow the steps below to set up and run the pipeline.

---

## 1. Prerequisites

### Install Docker
Docker is required to run LocalStack, a tool simulating AWS services locally.

1. Install Docker:
```bash
sudo apt update
sudo apt install docker.io
```

2. Verify Docker installation:
```bash
docker --version
```

3. Install AWS CLI
AWS CLI is used to interact with LocalStack S3 buckets.

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

4. Verify that the installation worked

```bash
aws --version
```

5. Configure AWS CLI for LocalStack

```bash
aws configure
```

Enter the following values:
* AWS Access Key ID: root
* AWS Secret Access Key: root
* Default region name: us-east-1
* Default output format: json

6. Create LocalStack S3 buckets:

```bash
Copier le code
aws --endpoint-url=http://localhost:4566 s3 mb s3://raw
aws --endpoint-url=http://localhost:4566 s3 mb s3://staging
aws --endpoint-url=http://localhost:4566 s3 mb s3://curated
```

7. Install DVC
DVC is used for data version control and pipeline orchestration.

```bash
pip install dvc
```

```bash
dvc remote add -d localstack-s3 s3://
dvc remote modify localstack-s3 endpointurl http://localhost:4566
```

## 2. Repository Setup
Install Python Dependencies

```bash
pip install -r build/requirements.txt
```

Start LocalStack

```bash
bash scripts/start_localstack.sh
```

Download the Dataset from: [Salesforce WikiText Dataset](https://huggingface.co/datasets/Salesforce/wikitext)

```bash
from datasets import load_dataset
import os

dataset = load_dataset("Salesforce/wikitext", "wikitext-2-raw-v1")

os.makedirs('data/raw', exist_ok=True)

# 2. Créer les sous-dossiers locaux (train, test, dev)
os.makedirs('data/raw/train', exist_ok=True)
os.makedirs('data/raw/test', exist_ok=True)
os.makedirs('data/raw/dev', exist_ok=True)

# 3. Sauvegarde des données dans les sous-dossiers
# Écriture dans des fichiers locaux
with open('data/raw/train/train.txt', 'w') as f:
    f.write('\n'.join(dataset['train']['text']))

with open('data/raw/test/test.txt', 'w') as f:
    f.write('\n'.join(dataset['test']['text']))

with open('data/raw/dev/dev.txt', 'w') as f:
    f.write('\n'.join(dataset['validation']['text']))
```

Move the dataset into a data/raw folder.

## 3. Running the Pipeline

Unpack the dataset into a single CSV file in the raw bucket:

```bash
python build/unpack_to_raw.py --input_dir data/raw --bucket_name raw --output_file_name combined_raw.txt
```

Preprocess the data to clean, encode, split into train/dev/test, and compute class weights:

```bash
python src/preprocess_to_staging.py \
    --bucket_raw raw \
    --db_host localhost \
    --db_user root \
    --db_password mypassword \
    --input_file combined_raw.txt
```

Prepare the data for model training by tokenizing sequences:

```bash
python src/process_to_curated.py \
    --sqlite_db_path data.db \
    --mongo_uri mongodb://localhost:27017/ \
    --mongo_db_name curated \
    --mongo_collection_name wikitext \
    --model_name distilbert-base-uncasedcsv
```


Verify in mongodb: 

```bash
brew tap mongodb/brew
brew install mongodb-database-tools

docker exec -it mongodbmongo #doesn't work 
# i used mongosh instead

use curated

db.wikitext.find().limit(5).pretty()
```
## 4. Running the Entire Pipeline with DVC (doesn't work)
The pipeline stages are defined in dvc.yaml. Run the pipeline using:

```bash
dvc repro
```

## 5. Notes
Ensure LocalStack is running before executing any pipeline stage.
This pipeline illustrates a basic ETL flow for a data lake, preparing data from raw to curated for AI model training.
If you encounter any issues, ensure Docker, AWS CLI, and DVC are correctly configured.