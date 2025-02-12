import json
from kafka.consumer import KafkaConsumer

from configs import kafka_config

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my_consumer_group_2'
)
my_name = "staskut"
topic_names = [f'{my_name}_temperature_humidity_alerts',]
consumer.subscribe(topic_names)
try:
    while True:
        for message in consumer:
            print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Reading stopped.")
finally:
    consumer.close()
