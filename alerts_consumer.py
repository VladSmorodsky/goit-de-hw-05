from configs import QUEUE_PREFIX, TEMPERATURE_ALEARTS, HUMIDITY_ALERTS
from sensor_consumer import create_consumer

if __name__ == '__main__':
    consumer = create_consumer(f"{QUEUE_PREFIX}_alerts_group")

    consumer.subscribe([TEMPERATURE_ALEARTS, HUMIDITY_ALERTS])

    try:
        for message in consumer:
            print(
                f"Received alert from topic '{message.topic}': {message.value}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()
