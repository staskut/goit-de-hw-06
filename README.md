1. `create_topics.py` creates necessary Kafka topics.
2. `run_sensor.py` mocks sensor work and sends data to the Kafka topic.
![1_running_two_sensors.png](screenshots/1_running_two_sensors.png)
3. `process_sensors_data.py` reads sensors data from the Kafka topic using Spark stream with a window, aggregates data and sends alerts to another Kafka topic if needed.
![2_spark_windows.png](screenshots/2_spark_windows.png)
4. `read_alerts.py` reads alerts from the Kafka topic.
5. ![3_alerts_read_from_kafka_topic.png](screenshots/3_alerts_read_from_kafka_topic.png)