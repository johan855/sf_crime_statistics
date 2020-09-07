from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    # Generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            json_lines = json.load(f)
            for line in json_lines:
                message = self.dict_to_binary(line)
                #Send the correct data
                self.send(self.topic, message)
                time.sleep(1)

    # Return the json dictionary to binary
    def dict_to_binary(self, line):
        binary = json.dumps(line).encode("utf-8")
        return binary
