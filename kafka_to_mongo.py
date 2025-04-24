from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import configparser
import logging

# Load config
config = configparser.ConfigParser()
config.read('Kafka/config_mongo.ini')

# Kafka & MongoDB config from config.ini
TOPIC = config.get('topics', 'destination_topic')
KAFKA_BROKER = config.get('kafka.local', 'bootstrap_servers').split(',')

MONGO_URI = config.get('mongo', 'uri')
MONGO_DB = config.get('mongo', 'database')
MONGO_COLLECTION = config.get('mongo', 'collection')

# Kafka consumer config
KAFKA_SECURITY_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': config.get('kafka.local', 'security_protocol'),
    'sasl_mechanism': config.get('kafka.local', 'sasl_mechanism'),
    'sasl_plain_username': config.get('kafka.local', 'sasl_plain_username'),
    'sasl_plain_password': config.get('kafka.local', 'sasl_plain_password'),
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': config.get('kafka.local', 'auto_offset_reset'),
    'group_id': config.get('kafka.local', 'group_id')
}

# Kafka consumer
consumer = KafkaConsumer(TOPIC, **KAFKA_SECURITY_CONFIG)

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
