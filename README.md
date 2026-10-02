# 🚍 Pipeline de Données de Transit en Temps Réel

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.13.4-005571?style=flat&logo=elasticsearch&logoColor=white)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-8.13.4-005571?style=flat&logo=kibana&logoColor=white)](https://www.elastic.co/kibana/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

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

```mermaid
flowchart TD
    API[Google Maps Directions API] -->|HTTP / Réponse JSON| Prod[Python Producer]
    Prod -->|Topic: navitia-raw| Kafka[(Apache Kafka)]
    Kafka -->|Streaming Consumer| Cons[Python Consumer<br/>Enrichissement geo_point]
    Cons -->|Bulk Indexation| ES[(Elasticsearch<br/>Index: navitia-data)]
    ES -->|Requêtes & Agrégations| Kibana[Kibana Dashboards & Maps]
