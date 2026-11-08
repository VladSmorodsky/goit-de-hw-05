from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topics, QUEUE_PREFIX

NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1

if __name__ == '__main__':
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    topic_list = []
    for topic_name in topics:
        new_topic = NewTopic(name=topic_name,
                             num_partitions=NUM_PARTITIONS,
                             replication_factor=REPLICATION_FACTOR)
        topic_list.append(new_topic)

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print(f"An error occurred: {e}")
    [print(topic)
     for topic in admin_client.list_topics() if QUEUE_PREFIX in topic]

    admin_client.close()
