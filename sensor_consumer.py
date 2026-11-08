from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer
from configs import kafka_config, BUILDING_SENSORS, TEMPERATURE_ALEARTS, HUMIDITY_ALERTS
from producer import create_producer

TEMPERATURE_MAX = 40
HUMIDITY_MIN = 20
HUMIDITY_MAX = 80


def create_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=JsonSerializer(),
        key_deserializer=JsonSerializer(),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id
    )


if __name__ == '__main__':
    producer = create_producer()
    consumer = create_consumer('my_sensor_group')

    consumer.subscribe([BUILDING_SENSORS])

    try:
        for message in consumer:
            print(
                f"Received message: {message.value} with key: {message.key}, partition {message.partition}")

            data = message.value
            temperature = data['temperature']
            humidity = data['humidity']

            if temperature > TEMPERATURE_MAX:
                alert = {
                    **data,
                    "message": f"Temperature {temperature} exceeds the limit of {TEMPERATURE_MAX}"
                }
                producer.send(TEMPERATURE_ALEARTS,
                              key=data['sensor_id'], value=alert)
                print(f"Alert for topic '{TEMPERATURE_ALEARTS}': {alert}")

            if humidity > HUMIDITY_MAX or humidity < HUMIDITY_MIN:
                alert = {
                    **data,
                    "message": f"Humidity {humidity} is outside the allowed range {HUMIDITY_MIN}-{HUMIDITY_MAX}"
                }
                producer.send(HUMIDITY_ALERTS,
                              key=data['sensor_id'], value=alert)
                print(f"Alert for topic '{HUMIDITY_ALERTS}': {alert}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
        producer.close()
