"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        # TODO (done): Configure the broker properties below.
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092", # Kafka broker address
            "schema.registry.url": "http://localhost:8081", # Schema Registry URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        #
        # TODO (done): Configure the AvroProducer
        #
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        #
        # TODO (done): Write code that creates the topic for this producer if it does not already exist on the Kafka Broker.
        #
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        
        topic_metadata = client.list_topics(timeout=5)
        if self.topic_name not in set(t.topic for t in iter(topic_metadata.topics.values())):
            topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
            )
            client.create_topics([topic])
            logger.info(f"Topic {self.topic_name} created successfully.")
        else:
            logger.info(f"Topic {self.topic_name} already exists.")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        
        #
        # TODO (done): Write cleanup code for the Producer here
        #
        if self.producer is not None:
            self.producer.flush()
            logger.info("Producer flushed and closed successfully.")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
