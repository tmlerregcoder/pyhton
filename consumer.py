from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'temperature_sensor_topic',
    api_version=(3,8,0),
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='temperature_sensor_consumer_group'
)

if __name__ == '__main__':
    for message in consumer:
        print(f"Received message: {message.value}")