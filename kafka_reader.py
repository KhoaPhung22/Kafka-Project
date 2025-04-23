# Description: Viết chương trình sử dụng Python thực hiện các yêu cầu sau:
# - Đọc dữ liệu từ nguồn Kafka được cung cấp trước và produce vào 1 topic trong Kafka bạn đã dựng
# - Thực hiện việc đọc dữ liệu từ topic mà bạn tạo ở bước trên và lưu trữ dữ liệu xuống MongoDB
# DoD: Source code github và kết quả chương trình đã chạy được //

from kafka import KafkaConsumer, KafkaProducer
import json

SOURCE_TOPIC = 'product_view'
DEST_TOPIC = 'khoa_destination_topic'
KAFKA_BROKER = [
    '113.160.15.232:9094',
    '113.160.15.232:9194',
    '113.160.15.232:9294'
]

# Configuration
KAFKA_SECURITY_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'kafka',
    'sasl_plain_password': 'UnigapKafka@2024',
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': 'earliest',
    'group_id': 'khoa-source-group'
}

KAFKA_PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'kafka',
    'sasl_plain_password': 'UnigapKafka@2024',
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'linger_ms': 100  # Optional: buffer slight delay for better batching
}

# Kafka consumer and producer
consumer = KafkaConsumer(SOURCE_TOPIC, **KAFKA_SECURITY_CONFIG)
producer = KafkaProducer(**KAFKA_PRODUCER_CONFIG)

print("Listening to source topic and forwarding to destination...")

try:
    for message in consumer:
        data = message.value
        print(f"Forwarding message: {data}")
        producer.send(DEST_TOPIC, value=data)
except KeyboardInterrupt:
    print("Stopped by user.")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    producer.flush()
    producer.close()
