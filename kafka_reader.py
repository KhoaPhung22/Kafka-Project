# Description: Viết chương trình sử dụng Python thực hiện các yêu cầu sau:
# - Đọc dữ liệu từ nguồn Kafka được cung cấp trước và produce vào 1 topic trong Kafka bạn đã dựng
# - Thực hiện việc đọc dữ liệu từ topic mà bạn tạo ở bước trên và lưu trữ dữ liệu xuống MongoDB
# DoD: Source code github và kết quả chương trình đã chạy được //

from kafka import KafkaConsumer, KafkaProducer
import json

SOURCE_TOPIC = 'product_view'
DEST_TOPIC = 'khoa_destination_topic'

# Remote Kafka broker (source)
REMOTE_KAFKA_BROKER = [
    '113.160.15.232:9094',
    '113.160.15.232:9194',
    '113.160.15.232:9294'
]

# Local Kafka broker (destination)
LOCAL_KAFKA_BROKER = [
    'localhost:9094',
    'localhost:9194',
    'localhost:9294'
    
]

# Remote Kafka (consumer) config
REMOTE_KAFKA_SECURITY_CONFIG = {
    'bootstrap_servers': REMOTE_KAFKA_BROKER,
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'kafka',
    'sasl_plain_password': 'UnigapKafka@2024',
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': 'earliest',
    'group_id': 'khoa-remote-consume-group'
}

# Local Kafka (producer) config
LOCAL_KAFKA_PRODUCER_CONFIG = {
    'bootstrap_servers': LOCAL_KAFKA_BROKER,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'admin',
    'sasl_plain_password': 'Unigap@2024'
}

# Consumer from remote broker
consumer = KafkaConsumer(SOURCE_TOPIC, **REMOTE_KAFKA_SECURITY_CONFIG)

# Producer to local broker
producer = KafkaProducer(**LOCAL_KAFKA_PRODUCER_CONFIG)

print("Forwarding messages from remote Kafka to local Kafka...")

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
