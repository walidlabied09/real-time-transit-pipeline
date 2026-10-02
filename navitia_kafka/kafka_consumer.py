import os
import sys
import json
import signal
import argparse
import logging
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from navitia_kafka.config import KAFKA

# ───── Configuration du logging ─────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | CONSUMER | %(levelname)s | %(message)s",
)

# ───── Connexion Elasticsearch ─────
es = Elasticsearch("http://localhost:9200")

# ───── Désérialisation sécurisée ─────
def _safe_deserialize(raw: bytes):
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        logging.warning("⚠️  Non-JSON message skipped: %r", raw)
        return None

# ───── Construction du consumer Kafka ─────
def build_consumer(topics, bootstrap, group_id):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=_safe_deserialize,
    )

# ───── Envoi dans Elasticsearch ─────
def store_to_elasticsearch(message: dict):
    if not isinstance(message, dict):
        return

    # Ajoute un champ "location" de type geo_point à partir de start_location
    if "start_location" in message and isinstance(message["start_location"], dict):
        lat = message["start_location"].get("lat")
        lng = message["start_location"].get("lng")
        if lat is not None and lng is not None:
            message["location"] = {"lat": lat, "lon": lng}

    try:
        es.index(index="navitia-data", document=message)
        logging.info("✅ Message indexé dans Elasticsearch.")
    except Exception as e:
        logging.error("❌ Erreur lors de l'indexation : %s", e)

# ───── Programme principal ─────
def main():
    parser = argparse.ArgumentParser(description="Navitia Kafka consumer")
    parser.add_argument(
        "--topics",
        default=os.getenv("KAFKA_TOPICS", KAFKA["topic"]).split(","),
        nargs="+",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA["bootstrap_servers"]),
    )
    parser.add_argument(
        "--group",
        default=os.getenv("KAFKA_GROUP_ID", KAFKA["group_id"]),
    )
    args = parser.parse_args()

    consumer = build_consumer(args.topics, args.bootstrap, args.group)

    def shutdown(*_):
        logging.info("🔚  Arrêt du consumer…")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logging.info("🎯 Listening → topics=%s, bootstrap=%s, group=%s", args.topics, args.bootstrap, args.group)

    for msg in consumer:
        if msg.value is None:
            continue
        logging.info(
            "📥 p%s/o%s → %s",
            msg.partition,
            msg.offset,
            json.dumps(msg.value, ensure_ascii=False),
        )
        store_to_elasticsearch(msg.value)

if __name__ == "__main__":
    main()
