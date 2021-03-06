from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import logging

logger = logging.getLogger(__name__)


class ProducerServer():
    def __init__(self):
        BOOTSTRAP_SERVERS = "PLAINTEXT://DESKTOP-B2QMGU6:9092"
        INPUT_FILE = "police-department-calls-for-service.json"
        TOPIC = "police.calls.service"
        self.topic = TOPIC
        self.bootstrap_servers = BOOTSTRAP_SERVERS
        self.input_file = INPUT_FILE
        self.num_partitions = 4
        self.replication_factor = 2
        self.progress_interval = 30
        self.admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    # Additional methods using the confluent-kafka library
    def create_topic(self):
        # TODO create case when topic does not exist so i dont have to create it manually
        futures = self.admin_client.create_topics([
            NewTopic(topic=self.topic,
                     num_partitions=self.num_partitions,
                     replication_factor=self.replication_factor)
        ])
        for topic_item, future in futures.items():
            try:
                future.result()
                print(f"Topic created: {topic_item}")
            except KafkaError as e:
                print(f"Kafka Error {topic_item}: {e}")

    # Return the json dictionary to binary
    def dict_to_binary(self, line):
        binary = json.dumps(line).encode("utf-8")
        return binary

    # Generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            json_lines = json.load(f)
            for line in json_lines:
                message = self.dict_to_binary(line)
                logger.info(f"dict_to_binary result message: {message}")
                #Send the correct data
                #self.send(self.topic, message)
                self.producer.produce(self.topic, value=message)
                #self.producer.poll(0)
                time.sleep(1)
            logger.debug("Flushing producer")
            self.producer.flush()
