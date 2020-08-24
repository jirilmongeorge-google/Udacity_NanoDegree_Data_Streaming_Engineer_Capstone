import producer_server
import pathlib
import json


def run_kafka_server():
    input_file = pathlib.Path(__file__).parent.joinpath("police-department-calls-for-service.json")
    print(input_file)
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police.calls",
        bootstrap_servers="localhost:9099",
        client_id="1"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
