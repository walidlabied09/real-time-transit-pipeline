# 🚍 Pipeline de Données de Transit en Temps Réel

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat\&logo=python\&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-231F20?style=flat\&logo=apachekafka\&logoColor=white)](https://kafka.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.13.4-005571?style=flat\&logo=elasticsearch\&logoColor=white)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-8.13.4-005571?style=flat\&logo=kibana\&logoColor=white)](https://www.elastic.co/kibana/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat\&logo=docker\&logoColor=white)](https://www.docker.com/)

Solution Big Data de bout en bout permettant la collecte, l'ingestion, le traitement, l'indexation et la visualisation en temps réel de données de transport interurbain.

---

## 📌 Présentation du Projet

Ce projet implémente un pipeline Big Data temps réel simulant et analysant les flux de transport entre plusieurs grandes métropoles françaises :

* Paris
* Lyon
* Marseille
* Toulouse
* Nantes
* Bordeaux
* Strasbourg
* Nice
* Grenoble

### Objectifs principaux

* **Ingestion continue** de données d'itinéraires via l'API Google Maps Directions.
* **Streaming et découplage** des flux de données avec Apache Kafka.
* **Enrichissement des données** avec un mapping géographique `geo_point`.
* **Indexation et recherche rapide** dans Elasticsearch.
* **Visualisation analytique** et cartographique avec Kibana.
* **Conteneurisation** de l'ensemble de l'infrastructure avec Docker Compose.

---

## 🏗️ Architecture du Pipeline

```text
┌─────────────────────┐
│   Python Producer   │
└──────────┬──────────┘
           │
           │ HTTP
           ▼
┌────────────────────────────┐
│ Google Maps Directions API │
└────────────┬───────────────┘
             │
             │ Réponse JSON
             ▼
┌─────────────────────┐
│    Apache Kafka     │
│   Topic: navitia-raw│
└──────────┬──────────┘
           │
           │ Consommation
           ▼
┌─────────────────────┐
│   Python Consumer   │
│  Enrichissement     │
│    geo_point        │
└──────────┬──────────┘
           │
           │ Indexation
           ▼
┌─────────────────────┐
│   Elasticsearch     │
│   Index: navitia-data│
└──────────┬──────────┘
           │
           │ Requêtes &
           │ Agrégations
           ▼
┌─────────────────────┐
│       Kibana        │
│ Dashboards & Maps   │
└─────────────────────┘
```

---

## 📁 Structure du Projet

```text
projet-bigdata-navitia/
│
├── navitia_kafka/
│   ├── config.py
│   ├── kafka_producer.py
│   └── kafka_consumer.py
│
├── docker-compose.yml
├── navitia-to-es.json
├── requirements.txt
└── README.md
```

### Description des fichiers

| Fichier              | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `config.py`          | Configuration des accès Kafka et de la clé API           |
| `kafka_producer.py`  | Appel de l'API et production des messages Kafka          |
| `kafka_consumer.py`  | Consommation, enrichissement et indexation Elasticsearch |
| `docker-compose.yml` | Orchestration des différents services                    |
| `navitia-to-es.json` | Configuration du connecteur Elasticsearch                |
| `requirements.txt`   | Dépendances Python du projet                             |
| `README.md`          | Documentation du projet                                  |

---

## ⚙️ Prérequis

Avant de lancer le projet, il faut disposer de :

* **Python 3.10+**
* **Docker**
* **Docker Compose**
* Une clé API valide pour **Google Maps Directions API**
* **Git** (facultatif, pour cloner le projet)

---

## 🚀 Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/walidlabied09/pipeline-de-transit-en-temps-r-el.git
cd pipeline-de-transit-en-temps-r-el/projet-bigdata-navitia
```

---

### 2. Créer l'environnement Python

Sous Windows :

```cmd
python -m venv venv
```

Activer l'environnement virtuel :

```cmd
venv\Scripts\activate
```

Installer les dépendances :

```cmd
pip install -r requirements.txt
```

Sous Linux/macOS :

```bash
source venv/bin/activate
pip install -r requirements.txt
```

---

## 🔑 Configuration

Modifier le fichier :

```text
navitia_kafka/config.py
```

Exemple :

```python
GOOGLE_API = {
    "key": "VOTRE_CLE_API_GOOGLE_MAPS"
}

KAFKA = {
    "bootstrap_servers": "localhost:29092",
    "topic": "navitia-raw",
    "group_id": "navitia-group",
}
```

> ⚠️ Ne publiez jamais votre véritable clé API Google dans un dépôt GitHub public.

---

## 🐳 Démarrage de l'infrastructure

Depuis le dossier contenant `docker-compose.yml` :

```cmd
docker-compose up -d
```

Pour vérifier les conteneurs :

```cmd
docker-compose ps
```

Pour voir les logs :

```cmd
docker-compose logs
```

Pour arrêter l'infrastructure :

```cmd
docker-compose down
```

---

## 🌐 Services

Après le démarrage, les services sont accessibles aux adresses suivantes :

| Service       | Adresse                 |
| ------------- | ----------------------- |
| Kafka         | `localhost:29092`       |
| Elasticsearch | `http://localhost:9200` |
| Kibana        | `http://localhost:5601` |
| Kafka Connect | `http://localhost:8083` |

---

## 🚦 Exécution du Pipeline

### 1. Démarrer le Consumer

Depuis la racine du projet :

```cmd
python -m navitia_kafka.kafka_consumer
```

Le Consumer :

1. Écoute le topic Kafka `navitia-raw`.
2. Récupère les messages JSON.
3. Enrichit les données géographiques.
4. Transforme les coordonnées en `geo_point`.
5. Envoie les documents vers Elasticsearch.
6. Utilise l'index `navitia-data`.

---

### 2. Démarrer le Producer

Dans un deuxième terminal :

```cmd
python -m navitia_kafka.kafka_producer
```

Le Producer :

1. Interroge l'API Google Maps Directions.
2. Récupère les informations des trajets.
3. Construit les messages JSON.
4. Publie les données dans Kafka.
5. Utilise le topic `navitia-raw`.

---

## 📊 Elasticsearch

Les données finales sont stockées dans l'index :

```text
navitia-data
```

Le champ géographique utilisé pour les visualisations est :

```json
{
  "location": {
    "lat": 48.8566,
    "lon": 2.3522
  }
}
```

Ce champ peut être utilisé comme type `geo_point` dans Elasticsearch.

---

## 📈 Visualisation avec Kibana

Ouvrir :

```text
http://localhost:5601
```

Créer ensuite un **Data View** basé sur :

```text
navitia-data*
```

### Visualisations possibles

#### 🗺️ Carte géographique

Visualisation de la distribution des points de départ et d'arrivée grâce au champ :

```text
location
```

#### 📊 Histogramme

Analyse du nombre de trajets par :

* Ville d'origine
* Ville de destination
* Durée
* Date

#### 🥧 Répartition

Analyse de la répartition des trajets selon les villes.

#### 🔢 KPI

Afficher notamment :

* Nombre total de trajets
* Durée moyenne
* Nombre de villes
* Nombre de destinations

---

## 🔄 Flux de données

```text
Google Maps API
      │
      ▼
Python Producer
      │
      ▼
Apache Kafka
      │
      │ Topic: navitia-raw
      ▼
Python Consumer
      │
      │ Enrichissement geo_point
      ▼
Elasticsearch
      │
      ▼
Kibana
      │
      ▼
Dashboards temps réel
```

---

## 🛠️ Stack Technique

| Technologie                | Utilisation                            |
| -------------------------- | -------------------------------------- |
| Python                     | Production et consommation des données |
| Apache Kafka               | Streaming et messaging                 |
| Zookeeper                  | Coordination Kafka                     |
| Kafka Connect              | Intégration avec Elasticsearch         |
| Elasticsearch              | Stockage et recherche                  |
| Kibana                     | Visualisation et dashboards            |
| Docker                     | Conteneurisation                       |
| Docker Compose             | Orchestration                          |
| Google Maps Directions API | Source des données                     |

---

## 🔐 Sécurité

Pour des raisons de sécurité :

* Ne pas publier les clés API.
* Ne pas stocker les secrets directement dans Git.
* Utiliser des variables d'environnement pour les informations sensibles.
* Ajouter les fichiers contenant des secrets dans `.gitignore`.

Exemple :

```text
.env
venv/
__pycache__/
*.pyc
```

---

## 🧪 Vérification du Pipeline

Vérifier Elasticsearch :

```cmd
curl http://localhost:9200
```

Vérifier les indices :

```cmd
curl http://localhost:9200/_cat/indices?v
```

Rechercher les données :

```cmd
curl http://localhost:9200/navitia-data/_search?pretty
```

Vérifier Kafka Connect :

```cmd
curl http://localhost:8083/connectors
```

---

## 👨‍💻 Auteur

**Walid Labied**

Projet Big Data — Pipeline de données de transit en temps réel.

GitHub :
https://github.com/walidlabied09

---

## 📄 Licence

Ce projet est réalisé dans un cadre académique et pédagogique.
