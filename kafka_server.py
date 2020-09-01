import producer_server

BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092"

def run_kafka_server():
	#get the json file path
    input_file = "police-department-calls-for-service.json"
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police.calls.service.",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="police_calls_service"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
