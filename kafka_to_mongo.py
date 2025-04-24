from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging


# Kafka & MongoDB config
TOPIC = 'khoa_destination_topic'
KAFKA_BROKER = [
    'localhost:9094',
    'localhost:9194',
    'localhost:9294'
    
]
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'kafka_db'
MONGO_COLLECTION = 'messages'

KAFKA_SECURITY_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'kafka',
    'sasl_plain_password': 'UnigapKafka@2024',
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': 'earliest',
    'group_id': 'khoa-source-mongo-group'
}

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    **KAFKA_SECURITY_CONFIG
)

# MongoDB client
client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]

print("Consuming from Kafka and writing to MongoDB...")

for msg in consumer:
    data = msg.value
    print(f"Inserting: {data}")
    try:
        collection.insert_one(data)
        print("Inserted successfully.")
    except Exception as e:
        print(f"Insert failed: {e}")
