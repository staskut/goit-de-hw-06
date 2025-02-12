import datetime
from random import randint
from time import sleep, time

from kafka import KafkaProducer


class Sensor:
    def __init__(self):
        self.sensor_id = randint(1, 1000)
        print(f"Initialized sensor with ID: {self.sensor_id}")
        self.producer = None
        self.topic_name = None

    def connect(self, kafka_config, topic_name):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_serializer=lambda v: str(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8')
        )
        self.topic_name = topic_name

    def measure(self):
        timestamp = datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
        temp = randint(25, 45)
        humidity = randint(15, 85)
        message = f"Time: {timestamp}, Sensor ID: {self.sensor_id}, Temperature: {temp}, Humidity: {humidity}"
        print(message)
        self.producer.send(self.topic_name, key=str(self.sensor_id), value=message)
        self.producer.flush()
        print(f"Message sent to topic '{self.topic_name}' successfully.")

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    from configs import kafka_config

    my_name = "staskut"
    topic_name = f'{my_name}_building_sensors'

    sensor = Sensor()
    sensor.connect(kafka_config, topic_name)

    try:
        while True:
            sensor.measure()
            sleep(1)
    except KeyboardInterrupt:
        print("Measurement stopped.")
    finally:
        sensor.close()