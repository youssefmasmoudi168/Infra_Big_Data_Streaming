from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Kafka connection details (ports from docker-compose)
BOOTSTRAP_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092']

# Topic configuration
TOPIC_NAME = "logistics-events"
NUM_PARTITIONS = 6
REPLICATION_FACTOR = 3

def create_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='topic_creator')
    
    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )
    
    try:
        admin_client.create_topics([topic])
        print(f"✅ Topic '{TOPIC_NAME}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic '{TOPIC_NAME}' already exists.")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_kafka_topic()
