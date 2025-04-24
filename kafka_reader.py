# Description: Viết chương trình sử dụng Python thực hiện các yêu cầu sau:
# - Đọc dữ liệu từ nguồn Kafka được cung cấp trước và produce vào 1 topic trong Kafka bạn đã dựng
# - Thực hiện việc đọc dữ liệu từ topic mà bạn tạo ở bước trên và lưu trữ dữ liệu xuống MongoDB
# DoD: Source code github và kết quả chương trình đã chạy được //

from kafka import KafkaConsumer, KafkaProducer
import json
import configparser

# Load config
config = configparser.ConfigParser()
config.read('Kafka/config.ini')

# Parse brokers
REMOTE_KAFKA_BROKER = config.get('kafka.remote', 'bootstrap_servers').split(',')
LOCAL_KAFKA_BROKER = config.get('kafka.local', 'bootstrap_servers').split(',')
SOURCE_TOPIC = config.get('topics', 'source_topic')
DEST_TOPIC = config.get('topics', 'destination_topic')

# Remote Kafka (consumer) config
REMOTE_KAFKA_SECURITY_CONFIG = {
    'bootstrap_servers': REMOTE_KAFKA_BROKER,
    'security_protocol': config.get('kafka.remote', 'security_protocol'),
    'sasl_mechanism': config.get('kafka.remote', 'sasl_mechanism'),
    'sasl_plain_username': config.get('kafka.remote', 'sasl_plain_username'),
    'sasl_plain_password': config.get('kafka.remote', 'sasl_plain_password'),
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'auto_offset_reset': config.get('kafka.remote', 'auto_offset_reset'),
    'group_id': config.get('kafka.remote', 'group_id')
}

# Local Kafka (producer) config
LOCAL_KAFKA_PRODUCER_CONFIG = {
    'bootstrap_servers': LOCAL_KAFKA_BROKER,
    'security_protocol': config.get('kafka.local', 'security_protocol'),
    'sasl_mechanism': config.get('kafka.local', 'sasl_mechanism'),
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'sasl_plain_username': config.get('kafka.local', 'sasl_plain_username'),
    'sasl_plain_password': config.get('kafka.local', 'sasl_plain_password'),
}


# Start consumer and producer
consumer = KafkaConsumer(SOURCE_TOPIC, **REMOTE_KAFKA_SECURITY_CONFIG)
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
