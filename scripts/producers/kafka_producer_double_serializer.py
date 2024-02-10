"""This module reads voltage data from a mini solar panel and sends it a Kafka Broker
as serialized floats.

Solar panel readings by default are in units of volts using a 3.3V reference.

This script is intended to be run on a Raspberry Pi.

This file is mostly focused on the software side of things i.e. sending the voltage
data to Kafka while the read_inputs.py module handles the hardware
side of things

To prevent an overload of messages from being sent to Kafka (and thus a huge bill)
I have added a sleep statement of 1 second between voltage readings.

This double_serializer script sends the voltage as just a serialized float
with no additional metadata.
The JSON_serializer module also included in this repo
 sends the data as a serialized JSON message instead.

Compared to the JSON script, this double_serializer script sends less complex
messages to Kafka.
For example, no Schema Registry is needed.

Produces 1 field to Kafka: 
1. Panel voltage. Float. Read from the mini solar panel.
Actually measured by the Raspberry Pi and analog-to-digital converter (ADC)
in this project
"""
# Standard Library Imports
import socket
import struct
from time import sleep

# 3rd Party Imports
from confluent_kafka import Producer

# Custom Modules Import
from credentials_management.credentials_funcs import retrieve_azure_secret
from hardware import read_inputs

# Globals
READINGS_SLEEP_TIME = 1  # second


def main():
    """main"""
    # Set up the Kafka Producer
    producer_config = get_producer_config()
    producer = Producer(producer_config)
    topic = "solar_panel_voltage_as_doubles"
    key = "solar_panel_readings"

    # Set up the ADC
    adc_channel = read_inputs.setup_adc_get_channel()

    # Main loop
    try:
        while True:
            voltage = read_inputs.get_single_input_voltage(adc_channel)
            produce_to_server(producer, topic, key, voltage)
            sleep(READINGS_SLEEP_TIME)
    except KeyboardInterrupt:
        pass
    producer.flush()


def get_producer_config() -> dict:
    """Sets configuration settings for the Kafka Broker / Producer

    For my configuration I have chosen to use Confluent Cloud.
    
    NOTE: Uses the Azure Key Vault to retrieve the server and API configuration
    details. Thus, Azure Key Vault will need to be configured before this function
    can be used.

    Returns the Kafka broker / producer configuration as a dictionary
    """
    bootstrap_server = retrieve_azure_secret('confluent-cloud-bootstrap-server-name')
    api_key = retrieve_azure_secret('confluent-bootstrap-server-api-key')
    api_secret = retrieve_azure_secret('confluent-cloud-server-api-secret')
    conf = {
        "bootstrap.servers": f"{bootstrap_server}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": f"{api_key}",
        "sasl.password": f"{api_secret}",
        "client.id": socket.gethostname(),
    }
    return conf


def callback(err, event):
    """Sends a delivery report when a message is recieved or message fails"""
    if err:
        print(f"Produce to topic {event.topic()} failed for event: {event.key()}")
    else:
        val = event.value().decode("utf8")
        print(f"{val} sent to partition {event.partition()}.")


def double_serializer(num: float):
    """Adaptation of the confluent_kafka DoubleSerializer Class

    Required because my Raspberry PI is only currently only working
    with the confluent_kafka library version 1.7.0.

    Adapted from documentation code at:
    https://docs.confluent.io/platform/6.0/clients/confluent-kafka-python/html/_modules/confluent_kafka/serialization.html

    Returns the voltage reading serialised into bytes
    """
    return struct.pack(">d", num)


def produce_to_server(producer, topic: str, key: str, voltage: float):
    """Sends the voltage data to the Confluent Cloud server"""
    producer.produce(
        topic=topic, key=key, value=double_serializer(voltage), on_delivery=callback
    )
    print(f"Produced voltage of {voltage}V!")


if __name__ == "__main__":
    main()
