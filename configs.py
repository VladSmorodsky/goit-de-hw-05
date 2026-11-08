import os

from dotenv import load_dotenv

load_dotenv()

kafka_config = {
    "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
    "username": os.environ["KAFKA_USERNAME"],
    "password": os.environ["KAFKA_PASSWORD"],
    "security_protocol": os.environ["KAFKA_SECURITY_PROTOCOL"],
    "sasl_mechanism": os.environ["KAFKA_SASL_MECHANISM"],
}

QUEUE_PREFIX = 'VS'

BUILDING_SENSORS = f"{QUEUE_PREFIX}_building_sensors"
TEMPERATURE_ALEARTS = f"{QUEUE_PREFIX}_temperature_alerts"
HUMIDITY_ALERTS = f"{QUEUE_PREFIX}_humidity_alerts"

topics = (BUILDING_SENSORS, TEMPERATURE_ALEARTS, HUMIDITY_ALERTS)
