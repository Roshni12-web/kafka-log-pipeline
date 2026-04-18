from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

events = ["login", "click", "purchase", "logout"]

while True:
    data = {
        "user_id": random.randint(1, 100),
        "event": random.choice(events),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("logs", value=data)
    print(f"Sent: {data}")

    time.sleep(2)