from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

faker = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3,8,0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_temperature_data():
    return {
        "sensor_id": str(  random.randint(1, 50)),
        "timestamp": faker.date_time_this_century().isoformat(),
        "temperature": random.uniform(0, 100),
        "location": faker.city()
    }
if __name__ == '__main__':    
    topic = 'temperature_sensor_topic'
    
    while True:
        data = generate_temperature_data()
        key = data['sensor_id']
        producer.send(topic, key=key, value=data)
        time.sleep(5)    
    
