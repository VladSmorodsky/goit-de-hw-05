from kafka import KafkaProducer
from kafka.serializer import JsonSerializer
import uuid
import time
import random
from configs import kafka_config, BUILDING_SENSORS


def create_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=JsonSerializer(),
        key_serializer=JsonSerializer()
    )


if __name__ == '__main__':
    producer = create_producer()
    sensor_id = str(uuid.uuid4())
    for sensor_value in range(30):
        try:
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "temperature": random.randint(25, 45),
                "humidity": random.randint(15, 85)
            }

            producer.send(BUILDING_SENSORS, key=str(uuid.uuid4()), value=data)
            producer.flush()

            print(
                f"Message {sensor_value} sent to topic '{BUILDING_SENSORS}': {data}")
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(2)
    producer.close()
