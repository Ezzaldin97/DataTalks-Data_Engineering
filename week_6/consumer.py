from kafka import KafkaConsumer
import json
import time


consumer = KafkaConsumer(
    'demo_1',
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = 'consumer.group.id.demo.3',
    value_deserializer = lambda x : json.loads(x.decode('utf-8'))
)

while(True):
    print('Reciving')
    for message in consumer:
        message = message.value
        print(message)
    time.sleep(1)