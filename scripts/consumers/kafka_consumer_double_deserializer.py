"""This module reads voltage data from a mini solar panel
that is sent to a Kafka broker by a Kafka Producer
(python script) on a Raspberry Pi.

Reads the data as serialized floats.

This module reads a serialised message from the producer as a float
and is intended to be paired with the producer script that sends only floats

Consumes one field from the Kafka broker:
1. Panel voltage. Float. Read from the mini solar panel.
Actually measured by the Raspberry Pi and analog-to-digital converter (ADC)
in this project

This script is an alternative to the json_deserializer script
and allows for a simpler setup and configuration.
"""
import socket
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.serialization import (
    DoubleDeserializer,
    SerializationContext,
    MessageField,
)


def main():
    """main"""
    # Set up Kafka broker and consumer configs
    topic = "solar_panel_voltage_as_doubles"
    conf = get_consumer_config()
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    # Set up deserializer
    double_deserializer = DoubleDeserializer()

    # Main Loop
    while True:
        try:
            event = consumer.poll(1.0)
            if event is None:
                continue
            consume_voltage_reading(event, topic, double_deserializer)
        except KeyboardInterrupt:
            break
    consumer.close()


def consume_voltage_reading(event, topic: str, double_deserializer):
    """Consumes and deserializes the voltage reading"""
    actual_voltage_v = double_deserializer(
        event.value(), SerializationContext(topic, MessageField.VALUE)
    )
    raw_timestamp = event.timestamp()
    actual_timestamp = datetime.fromtimestamp(raw_timestamp[1] // 1000)
    if actual_voltage_v is not None:
        print(f"Current voltage at {actual_timestamp} is {actual_voltage_v} volts")


def get_consumer_config() -> dict:
    """Sets the configuration settings for the Kafka consumer

    For my configuration I have chosen to use Confluent Cloud
    
    NOTE: Uses the Azure Key Vault to retrieve the server and API configuration
    details. Thus, Azure Key Vault will need to be configured before this function
    can be used.
    
    Returns the Kafka broker / consumer configuration as a dictionary
    """
    bootstrap_server = retrieve_azure_secret('confluent-cloud-bootstrap-server-name')
    api_key = retrieve_azure_secret('confluent-bootstrap-server-api-key')
    api_secret = retrieve_azure_secret('confluent-cloud-server-api-secret')
    
    consumer_config = {
        "bootstrap.servers": f"{bootstrap_server}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": f"{api_key}",
        "sasl.password": f"{api_secret}",
        "client.id": socket.gethostname(),
        "group.id": "solar_panel_voltage_app",
        "auto.offset.reset": "earliest",
    }
    return consumer_config


if __name__ == "__main__":
    main()
