from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)
topics = admin_client.list_topics()

my_name = "staskut"
for n in ["building_sensors", "temperature_alerts", "humidity_alerts", "temperature_humidity_alerts"]:
    topic_name = f'{my_name}_{n}'
    num_partitions = 2
    replication_factor = 1

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        if topic_name in topics:
            admin_client.delete_topics([topic_name])
            print(f"Topic '{topic_name}' deleted successfully.")
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

print([t for t in admin_client.list_topics() if my_name in t])

admin_client.close()

