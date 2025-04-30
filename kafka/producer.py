from kafka import KafkaProducer
import time, json
from datetime import datetime
import logging
import sys

sys.path.append(".")
from ingestion.kafka_ingestion import get_arrivals, get_disruptions_for_line

topic1 = "arrival"
topic2 = "disruption"

line_json = None
path = "line_stop.json"
with open(path, "r") as f:
    line_json = json.load(f)
line_ids = []
naptan_idx = []
for key, values in line_json.items():
    for key, values in line_json[key].items():
        line_ids.append(key)
        naptan_idx.extend(values)

producer = KafkaProducer(
    bootstrap_servers = '127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



while True:
    for naptan_id in naptan_idx:
        data = get_arrivals(naptan_id)
        if data:
            producer.send(topic1, data)
    for line_id in line_ids:
        data = get_disruptions_for_line(line_id)
        if data:
            producer.send(topic2, data)
    print(f"Arrival and Disruption data produced at {datetime.now()}")
    