import producer_server


def run_kafka_server():
    #get the json file path
    producer = producer_server.ProducerServer()
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
