# TP3 - Pipeline de traitement des données
Ce projet implémente une pipeline de traitement de données en trois étapes utilisant DVC (Data Version Control) pour l'orchestration.

## Architecture
La pipeline est composée de trois étapes principales :

* Raw : Extraction des données sources vers une zone brute
* Staging : Transformation et chargement dans MySQL
* Curated : Traitement final et stockage dans MongoDB


## Prérequis
Python 3.8+
DVC
MySQL
MongoDB
LocalStack (pour simuler S3)


## Configurez vos services :
MySQL sur localhost:3306
MongoDB sur localhost:27017
LocalStack sur localhost:4566

## Pipeline DVC
La pipeline est définie dans dvc.yaml et comprend trois étapes :

unpack_to_raw : Extrait les données vers data/raw
preprocess_to_staging : Charge les données dans MySQL
process_to_curated : Traite et stocke dans MongoDB