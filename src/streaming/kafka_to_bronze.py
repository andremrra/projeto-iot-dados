from kafka import KafkaConsumer
import json
from datetime import datetime
import os

consumer = KafkaConsumer(
    'iot-sensors',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("📥 Consumindo eventos do Kafka...")

for message in consumer:

    data = message.value

    now = datetime.now()

    path = (
        f"data/bronze/"
        f"year={now.year}/"
        f"month={now.month}/"
        f"day={now.day}"
    )

    os.makedirs(path, exist_ok=True)

    filename = f"{path}/events.json"

    with open(filename, "a") as f:
        f.write(json.dumps(data) + "\n")

    print("✅ Evento salvo:", data["sensor_id"])