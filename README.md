# 🚀 Kafka Real-Time Data Pipeline

## 📌 Overview

This project demonstrates a fault-tolerant real-time data pipeline using Kafka, Python, and MySQL.

## ⚙️ Architecture

Producer → Kafka → Consumer → MySQL

## 🔧 Features

* Real-time event streaming using Kafka
* Batch processing for performance optimization
* Deduplication using unique constraints
* Retry mechanism for fault tolerance
* Logging for monitoring and debugging
* Failure simulation and recovery testing

## 🛠️ Tech Stack

* Apache Kafka
* Python (kafka-python)
* MySQL
* Docker

## ▶️ How to Run

### Start Kafka

```bash
docker-compose up -d
```

### Run Producer

```bash
python producer.py
```

### Run Consumer

```bash
python consumer.py
```

## 🧪 Failure Testing

* Stop MySQL service
* Observe retry mechanism
* Restart MySQL and verify recovery

## 📊 Future Improvements

* Add Spark Streaming
* Deploy on AWS
* Add Airflow
