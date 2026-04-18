from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime
import logging
import time

# 🔹 Logging setup
logging.basicConfig(
    filename='consumer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 🔹 MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Roshni@123",   # 👈 replace
    database="kafka_db"
)

cursor = conn.cursor()

# 🔹 Kafka Consumer
consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='final-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("🚀 Consumer started... Listening to Kafka topic 'logs'")

# 🔹 Batch config
batch = []
BATCH_SIZE = 10

# 🔁 Retry function (FINAL VERSION)
def insert_with_retry(query, values, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            cursor.executemany(query, values)
            conn.commit()
            print(f"✅ Insert success on attempt {attempt}")
            return True

        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            logging.error(f"Attempt {attempt} failed: {e}")

            if attempt < retries:
                print(f"⏳ Retrying in {delay} seconds...")
                time.sleep(delay)

    print("❌ All retry attempts failed")
    logging.error("All retry attempts failed")
    return False

# 🔹 Consume messages
for message in consumer:
    try:
        raw_data = message.value

        # 🔹 Safe JSON parsing
        data = json.loads(raw_data)

        user_id = int(data["user_id"])
        event = data["event"]
        timestamp = datetime.fromisoformat(data["timestamp"])

        # 🔹 Add to batch
        batch.append((user_id, event, timestamp))

        # 🔹 Batch insert
        if len(batch) >= BATCH_SIZE:
            query = """
                INSERT IGNORE INTO logs (user_id, event, timestamp)
                VALUES (%s, %s, %s)
            """

            success = insert_with_retry(query, batch)

            if success:
                print(f"📦 Inserted batch of {len(batch)} records")
                logging.info(f"Inserted batch of {len(batch)} records")
                batch.clear()
            else:
                print("❌ Batch insert failed after retries")
                logging.error("Batch insert failed after retries")

    except json.JSONDecodeError:
        print(f"⚠️ Skipping non-JSON message: {raw_data}")
        logging.warning(f"Invalid JSON skipped: {raw_data}")

    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Error: {e}")