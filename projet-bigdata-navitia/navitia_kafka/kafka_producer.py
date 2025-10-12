# navitia_kafka/google_producer.py

import os, json, time, logging
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from navitia_kafka.config import GOOGLE_API, KAFKA

# ───── Logging ─────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | PRODUCER | %(levelname)s | %(message)s",
)

# ───── Kafka Producer ─────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA["bootstrap_servers"]],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ───── Liste de trajets à simuler ─────
routes = [
    {"origin": "Lyon", "destination": "Paris"},
    {"origin": "Toulouse", "destination": "Marseille"},
    {"origin": "Nantes", "destination": "Bordeaux"},
    {"origin": "Lille", "destination": "Strasbourg"},
    {"origin": "Nice", "destination": "Grenoble"},
]

# ───── Requête API Google Directions ─────
def get_directions(origin, destination, mode="transit"):
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "key": GOOGLE_API["key"]
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        if r.json().get("status") != "OK":
            logging.warning("❌ API Error (%s → %s): %s", origin, destination, r.json().get("status"))
            return None
    except Exception as e:
        logging.warning("🌐 Request failed (%s → %s): %s", origin, destination, e)
        return None

    leg = r.json()["routes"][0]["legs"][0]

    return {
        "origin": origin,
        "destination": destination,
        "duration_minutes": leg["duration"]["value"] // 60,
        "start_location": leg["start_location"],
        "end_location": leg["end_location"],
        "mode": mode,
        "datetime": datetime.now(timezone.utc).isoformat(),
        "stop_area": f"gare de {destination.lower().strip()}"
    }

# ───── Envoi boucle Kafka ─────
def main():
    count = 0
    max_iterations = 20  # ➕ Modifie ce nombre si tu veux plus de données
    try:
        for _ in range(max_iterations):
            for route in routes:
                data = get_directions(route["origin"], route["destination"])
                if data:
                    producer.send(KAFKA["topic"], data)
                    count += 1
                    logging.info("📤 [%s] Envoyé : %s → %s", count, data["origin"], data["destination"])
                    time.sleep(1)
        producer.flush()
        logging.info("✅ %s messages envoyés avec succès.", count)
    except KeyboardInterrupt:
        logging.info("🛑 Interruption manuelle. Total envoyé : %s", count)

if __name__ == "__main__":
    main()
