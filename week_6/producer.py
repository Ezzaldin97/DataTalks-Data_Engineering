import time
import json
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                         value_serializer = lambda x: json.dumps(x).encode('utf-8'))

for num in range(1000):
    data = {"number":num}
    producer.send('demo_1', value = data)
    print('Producing')
    time.sleep(1)