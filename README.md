
# 🚍 Pipeline de Données de Transit en Temps Réel

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.13.4-005571?style=flat&logo=elasticsearch&logoColor=white)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-8.13.4-005571?style=flat&logo=kibana&logoColor=white)](https://www.elastic.co/kibana/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

> **Solution Big Data end-to-end pour la collecte, l'ingestion, le traitement, l'indexation et la visualisation en temps réel de données de transport interurbain.**

---

## 📌 Présentation du Projet

Ce projet implémente un pipeline **Big Data temps réel** permettant de simuler et d'analyser les flux de transport entre plusieurs grandes métropoles françaises.

### 🏙️ Villes couvertes

- 🇫🇷 Paris
- 🇫🇷 Lyon
- 🇫🇷 Marseille
- 🇫🇷 Toulouse
- 🇫🇷 Nantes
- 🇫🇷 Bordeaux
- 🇫🇷 Strasbourg
- 🇫🇷 Nice
- 🇫🇷 Grenoble

### 🎯 Objectifs principaux

- 🔄 **Ingestion continue** des données d'itinéraires via l'API Google Maps Directions.
- 📡 **Streaming des données** avec Apache Kafka.
- 🐍 **Traitement et enrichissement** avec Python.
- 🌍 **Enrichissement géographique** avec le format `geo_point`.
- ⚡ **Indexation rapide** dans Elasticsearch.
- 📊 **Analyse et visualisation** avec Kibana.
- 🗺️ **Visualisation cartographique** des données de transport.
- 🐳 **Conteneurisation** de l'ensemble de l'infrastructure avec Docker Compose.

---

## 🏗️ Architecture du Pipeline

```mermaid
flowchart TD
    API[Google Maps Directions API] -->|HTTP / JSON| Prod[Python Producer]
    Prod -->|Topic: navitia-raw| Kafka[(Apache Kafka)]
    Kafka -->|Streaming Consumer| Cons[Python Consumer]
    Cons -->|Enrichissement geo_point| Geo[Geo Enrichment]
    Geo -->|Bulk Indexation| ES[(Elasticsearch)]
    ES -->|Requêtes & Agrégations| Kibana[Kibana]
    Kibana --> Maps[Dashboards & Maps]
````

### 🔄 Flux de données

```text
┌──────────────────────────────┐
│ Google Maps Directions API   │
└──────────────┬───────────────┘
               │
               │ HTTP / JSON
               ▼
┌──────────────────────────────┐
│       Python Producer        │
│   Collecte des itinéraires   │
└──────────────┬───────────────┘
               │
               │ Production
               ▼
┌──────────────────────────────┐
│        Apache Kafka          │
│      Topic: navitia-raw      │
└──────────────┬───────────────┘
               │
               │ Consommation
               ▼
┌──────────────────────────────┐
│       Python Consumer        │
│  Nettoyage & enrichissement  │
└──────────────┬───────────────┘
               │
               │ geo_point
               ▼
┌──────────────────────────────┐
│       Elasticsearch          │
│     Index: navitia-data      │
└──────────────┬───────────────┘
               │
               │ Requêtes
               ▼
┌──────────────────────────────┐
│           Kibana             │
│    Dashboards & Maps         │
└──────────────────────────────┘
```

---

## 📁 Structure du Projet

```text
real-time-transit-pipeline/
│
├── navitia_kafka/
│   ├── __init__.py
│   ├── config.py
│   ├── kafka_producer.py
│   └── kafka_consumer.py
│
├── docker-compose.yml
├── navitia-to-es.json
├── requirements.txt
├── .gitignore
├── .env.example
├── LICENSE
└── README.md
```

### 📄 Description des fichiers

| Fichier                           | Description                                                                              |
| --------------------------------- | ---------------------------------------------------------------------------------------- |
| `navitia_kafka/config.py`         | Configuration des accès Kafka, Elasticsearch et de la clé API                            |
| `navitia_kafka/kafka_producer.py` | Appel de l'API et production des messages Kafka                                          |
| `navitia_kafka/kafka_consumer.py` | Consommation, enrichissement géographique et indexation Elasticsearch                    |
| `docker-compose.yml`              | Déploiement multi-conteneurs de Kafka, Zookeeper, Kafka Connect, Elasticsearch et Kibana |
| `navitia-to-es.json`              | Configuration du connecteur Kafka Connect vers Elasticsearch                             |
| `requirements.txt`                | Dépendances Python nécessaires au projet                                                 |
| `.env.example`                    | Gabarit des variables d'environnement                                                    |
| `.gitignore`                      | Fichiers exclus du dépôt Git                                                             |
| `LICENSE`                         | Licence MIT du projet                                                                    |
| `README.md`                       | Documentation complète du projet                                                         |

---

# ⚙️ Prérequis

Avant de lancer le projet, assurez-vous de disposer de :

* **Python 3.10+**
* **Docker**
* **Docker Compose**
* **Git**
* Une clé API valide pour **Google Maps Directions API**

### Vérifier Python

```bash
python --version
```

### Vérifier Docker

```bash
docker --version
docker compose version
```

### Vérifier Git

```bash
git --version
```

---

# 🚀 Installation & Démarrage

## 1. Cloner le dépôt

```bash
git clone https://github.com/walidlabied09/real-time-transit-pipeline.git
cd real-time-transit-pipeline
```

---

## 2. Créer l'environnement virtuel

### Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

---

# 🔐 Configuration

## 1. Créer le fichier `.env`

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

---

## 2. Configurer les variables d'environnement

Exemple :

```env
GOOGLE_MAPS_API_KEY=VOTRE_CLE_API_GOOGLE_MAPS
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=navitia-raw
KAFKA_GROUP_ID=navitia-group
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX=navitia-data
```

Selon l'implémentation actuelle du projet, les paramètres peuvent également être définis dans :

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

> ⚠️ **Sécurité :** ne publiez jamais votre véritable clé API, mot de passe ou token dans un dépôt GitHub public.

---

# 🐳 Démarrage de l'infrastructure Docker

Lancer tous les services :

```bash
docker compose up -d
```

Vérifier le statut des conteneurs :

```bash
docker compose ps
```

Afficher les logs :

```bash
docker compose logs -f
```

Afficher les logs d'un service spécifique :

```bash
docker compose logs -f kafka
```

Arrêter l'infrastructure :

```bash
docker compose down
```

Supprimer également les volumes :

```bash
docker compose down -v
```

---

# 🌐 Services Disponibles

Une fois les conteneurs démarrés :

| Service           | Adresse locale          |
| ----------------- | ----------------------- |
| **Kafka Broker**  | `localhost:29092`       |
| **Elasticsearch** | `http://localhost:9200` |
| **Kibana**        | `http://localhost:5601` |
| **Kafka Connect** | `http://localhost:8083` |

---

# 🚦 Exécution du Pipeline

Le pipeline fonctionne selon le flux suivant :

```text
Google Maps API
      │
      ▼
Python Producer
      │
      ▼
Apache Kafka
      │
      ▼
Python Consumer
      │
      ▼
Elasticsearch
      │
      ▼
Kibana
```

---

## 1️⃣ Démarrer le Consumer

Ouvrir un premier terminal avec l'environnement virtuel activé :

```bash
python -m navitia_kafka.kafka_consumer
```

Le Consumer :

1. Se connecte à Kafka.
2. Écoute le topic `navitia-raw`.
3. Récupère les messages JSON.
4. Extrait les informations géographiques.
5. Enrichit les données avec un champ `geo_point`.
6. Indexe les documents dans Elasticsearch.

---

## 2️⃣ Démarrer le Producer

Ouvrir un deuxième terminal :

```bash
python -m navitia_kafka.kafka_producer
```

Le Producer :

1. Interroge l'API Google Maps.
2. Récupère les informations d'itinéraire.
3. Transforme les données en JSON.
4. Publie les messages dans Kafka.
5. Répète automatiquement la collecte selon l'intervalle configuré.

---

# 📡 Apache Kafka

Le topic principal utilisé par le pipeline est :

```text
navitia-raw
```

### Exemple de message

```json
{
  "origin": "Paris",
  "destination": "Lyon",
  "duration_seconds": 16200,
  "distance_meters": 465000,
  "timestamp": "2026-10-02T12:00:00Z"
}
```

Kafka assure le rôle de couche de streaming entre la collecte et le traitement des données.

```text
Google Maps API
       │
       ▼
Python Producer
       │
       ▼
Apache Kafka
       │
       ▼
Python Consumer
```

Cette architecture permet de découpler la production des données de leur consommation.

---

# 🔎 Elasticsearch

Les documents sont indexés dans :

```text
navitia-data
```

### Exemple de document Elasticsearch

```json
{
  "origin": "Paris",
  "destination": "Lyon",
  "duration_seconds": 16200,
  "distance_meters": 465000,
  "location": {
    "lat": 48.8566,
    "lon": 2.3522
  },
  "timestamp": "2026-10-02T12:00:00Z"
}
```

Le champ :

```json
"location": {
  "lat": 48.8566,
  "lon": 2.3522
}
```

permet d'exploiter les données géographiques dans Kibana.

---

# 🗺️ Visualisation avec Kibana

Ouvrir Kibana :

```text
http://localhost:5601
```

Puis accéder à :

```text
Stack Management
        ↓
Data Views
        ↓
Create data view
```

Créer une Data View basée sur :

```text
navitia-data*
```

Selon la version de Kibana, la fonctionnalité peut également être appelée :

```text
Index Patterns
```

---

## 📊 Dashboards recommandés

### 🗺️ Carte géographique

Utiliser le champ :

```text
location
```

pour visualiser les données sur une carte.

### 📈 Analyses temporelles

Créer des visualisations pour analyser :

* Durée moyenne des trajets
* Distance moyenne
* Évolution des temps de parcours
* Nombre de trajets par période
* Évolution des données dans le temps

### 🔢 KPIs

Exemples de KPIs :

* Nombre total de trajets
* Nombre de trajets par ville
* Distance moyenne
* Durée moyenne
* Origine → destination
* Évolution des trajets
* Flux entre les métropoles

---

# 🧪 Tests & Vérification

## Vérifier Elasticsearch

```bash
curl http://localhost:9200
```

Vérifier les indices :

```bash
curl http://localhost:9200/_cat/indices?v
```

---

## Rechercher les documents

### Windows

```powershell
curl.exe -X GET "http://localhost:9200/navitia-data/_search?pretty" -H "Content-Type: application/json" -d "{\"query\":{\"match_all\":{}}}"
```

### Linux / macOS

```bash
curl -X GET "http://localhost:9200/navitia-data/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match_all":{}}}'
```

---

# 🔌 Vérification de Kafka Connect

Lister les connecteurs :

```bash
curl http://localhost:8083/connectors
```

Vérifier le statut d'un connecteur :

```bash
curl http://localhost:8083/connectors/navitia-elasticsearch/status
```

---

# 🔄 Architecture Kafka Connect

Le fichier :

```text
navitia-to-es.json
```

contient la configuration du connecteur Kafka Connect destiné à Elasticsearch.

Architecture :

```text
┌──────────────────┐
│  Apache Kafka    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Kafka Connect   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Elasticsearch    │
└──────────────────┘
```

---

# 🛠️ Stack Technique

| Technologie         | Rôle dans l'architecture                         |
| ------------------- | ------------------------------------------------ |
| **Python**          | Collecte, production, consommation et traitement |
| **Apache Kafka**    | Streaming distribué et découplage des flux       |
| **Zookeeper**       | Coordination de Kafka                            |
| **Kafka Connect**   | Intégration Kafka → Elasticsearch                |
| **Elasticsearch**   | Indexation et recherche des données              |
| **Kibana**          | Dashboards, analytics et cartographie            |
| **Docker Compose**  | Conteneurisation et orchestration                |
| **Google Maps API** | Source des données d'itinéraires                 |

---

# 🧠 Compétences mises en œuvre

## Data Engineering

* API Data Collection
* Data Ingestion
* Data Streaming
* Data Processing
* Data Transformation
* Data Enrichment
* Data Indexing

## Big Data

* Apache Kafka
* Kafka Connect
* Elasticsearch
* Kibana
* Zookeeper

## Python

* REST API
* JSON
* Kafka Producer
* Kafka Consumer
* Elasticsearch Client

## DevOps

* Docker
* Docker Compose
* Variables d'environnement
* Services conteneurisés

## Data Visualization

* Kibana Dashboards
* Elasticsearch Aggregations
* Geo-spatial Visualization
* Real-Time Analytics

---

# 🔒 Sécurité

Les informations sensibles ne doivent jamais être publiées sur GitHub :

```text
API Keys
Passwords
Tokens
Credentials
.env
```

Exemple de `.gitignore` :

```gitignore
.env
.venv/
__pycache__/
*.pyc
.idea/
.vscode/
```

---

# 🐛 Dépannage

## Kafka ne démarre pas

Vérifier les conteneurs :

```bash
docker compose ps
```

Afficher les logs :

```bash
docker compose logs kafka
```

---

## Elasticsearch ne répond pas

Tester :

```bash
curl http://localhost:9200
```

Puis consulter les logs :

```bash
docker compose logs elasticsearch
```

---

## Kibana ne s'affiche pas

Vérifier les conteneurs :

```bash
docker compose ps
```

Puis :

```bash
docker compose logs kibana
```

Elasticsearch doit être complètement démarré avant que Kibana puisse fonctionner correctement.

---

## Aucun document dans Elasticsearch

Vérifier les éléments suivants :

```text
1. Docker est démarré
        ↓
2. Kafka est opérationnel
        ↓
3. Elasticsearch est opérationnel
        ↓
4. Producer actif
        ↓
5. Topic navitia-raw reçoit des messages
        ↓
6. Consumer actif
        ↓
7. Index navitia-data contient des documents
        ↓
8. Kibana peut visualiser les données
```

---

# 📌 Flux complet du projet

```text
                         ┌─────────────────────────┐
                         │ Google Maps Directions  │
                         │          API            │
                         └────────────┬────────────┘
                                      │
                                      │ HTTP / JSON
                                      ▼
                         ┌─────────────────────────┐
                         │    Python Producer      │
                         └────────────┬────────────┘
                                      │
                                      │ Messages
                                      ▼
                         ┌─────────────────────────┐
                         │      Apache Kafka        │
                         │     Topic: navitia-raw   │
                         └────────────┬────────────┘
                                      │
                                      │ Streaming
                                      ▼
                         ┌─────────────────────────┐
                         │    Python Consumer      │
                         │                         │
                         │  Data Processing        │
                         │  Data Enrichment        │
                         │  geo_point              │
                         └────────────┬────────────┘
                                      │
                                      │ Indexation
                                      ▼
                         ┌─────────────────────────┐
                         │     Elasticsearch       │
                         │     navitia-data        │
                         └────────────┬────────────┘
                                      │
                                      │ Queries
                                      ▼
                         ┌─────────────────────────┐
                         │         Kibana          │
                         │                         │
                         │ Dashboards              │
                         │ Maps                    │
                         │ Analytics               │
                         └─────────────────────────┘
```

---

# 📈 Évolutions possibles

Le projet peut être étendu avec :

* ⚡ Apache Spark Structured Streaming
* 📊 Power BI
* 🤖 Machine Learning pour la prédiction des temps de trajet
* 🚨 Détection automatique des anomalies
* ☁️ Déploiement sur Microsoft Azure
* ☁️ Déploiement sur AWS
* 📦 Kubernetes
* 📡 Plusieurs partitions Kafka
* 🔐 Authentification et sécurisation des services
* 📈 Prometheus et Grafana pour le monitoring
* 🗺️ Analyses géospatiales avancées
* ⚙️ Optimisation des performances du pipeline

---

# 👨‍💻 Auteur

**Walid Labied**

**Data Engineer | Big Data, Streaming & Analytics**

GitHub : [@walidlabied09](https://github.com/walidlabied09)

---

# 📄 Licence

Ce projet est distribué sous licence **MIT**.

Consultez le fichier [`LICENSE`](LICENSE) pour plus d'informations.

---

⭐ **Si ce projet vous intéresse, n'hésitez pas à explorer le dépôt et à laisser une étoile sur GitHub.**

```
```
