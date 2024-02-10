"""This module reads voltage data from a mini solar panel and sends it a Kafka Broker
as serialized JSON.

Solar panel readings by default are in units of volts using a 3.3V reference.

This script is intended to be run on a Raspberry Pi.

This file is mostly focused on the software side of things i.e. sending the voltage
data to Kafka while the read_inputs.py module handles the hardware
side of things

To prevent an overload of messages from being sent to Kafka (and thus a huge bill)
I have added a sleep statement of 1 second between voltage readings.

This module sends a JSON serialised message.
It is intended to be paired with the consumer script for JSON.
The double_serializer script (included in this repository) sends just voltage as just
a serialized float.

Compared to the double_serializer script also included in this repo,
this JSON script allows for sending more
complex messages to Kafka.
However, it is also means that this script should use the Kafka Schema Registry.

This script produces 3 fields in the serialized JSON to Kafka:
1. voltage_volts. Float. Voltage in volts read from the solar panel.
This is directly measured by the Raspberry Pi and 
analog-to-digital converter (ADC) in this project.

2. panel_producing. String. Represents which solar panel is producing the reading.
My hobby project only uses 1 panel but
I wanted to add this for fun, to get more data types in my JSON, and
replicate a JSON message closer to what might be sent in real life.
My project doesn't actually measure this - just for fun.

3. Connection Ok. Boolean. Represents whether the connection with the panel is ok.
Again, just added for fun,
to get more data types in my JSON, and
replicate a JSON message closer to what might be sent in real life.
My project doesn't actually measure this - just for fun. 
"""
# Standard Library Imports
import socket
import sys
from time import sleep

# 3rd Party Imports
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


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
    topic = "solar_panel_voltage_as_json"
    key = "solar_panel_voltage_readings"

    # Set up schema registry
    schema_registry_config = get_schema_registry_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    schema_str = get_schema_str()

    # Set up Serializer
    json_serializer = JSONSerializer(
        schema_str, schema_registry_client, voltage_to_dict
    )

    # Set up the ADC
    adc_channel = read_inputs.setup_adc_get_channel()

    # Main Production Loop
    try:
        while True:
            voltage = read_inputs.get_single_input_voltage(adc_channel)
            produce_to_server(producer, topic, key, voltage, json_serializer)
            print(f"Produced voltage of {voltage}V!")
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

def get_schema_registry_config() -> dict:
    """Sets configuration settings for the Kafka Schema Registry

    For my configuration I have chosen to use Confluent Cloud.
    
    NOTE 1: The schema should be defined on the topic using Confluent Cloud
    before using this function.
    
    NOTE 2: Uses the Azure Key Vault to retrieve the server and API configuration
    details. Thus, Azure Key Vault will need to be configured before this function
    can be used.
    
    Returns the Kafka Schema Registry configuration as a dictionary
    """
    schema_server = retrieve_azure_secret('confluent-cloud-schema-registry-server-name')
    schema_api_key = retrieve_azure_secret('confluent-cloud-schema-api-key')
    schema_api_secret = retrieve_azure_secret('confluent-cloud-schema-api-secret')
    sr_config = {
        "url": f"{schema_server}",
        "basic.auth.user.info": f"{schema_api_key}:{schema_api_secret}"
    }
    return sr_config


def delivery_report(err, event):
    """Sends a delivery report when a message is recieved or message fails"""
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}:{err}')
    else:
        print(f'Reading for {event.key().decode("utf8")} produced to {event.topic()}')


def get_schema_str() -> str:
    """Defines the schema that the produced JSON should adhere to

    NOTE: For my configuration I have chosen to use Confluent Cloud.
    The schema should be defined on the topic using Confluent Cloud before
    using this function.
    Using Confluent Cloud will also help create this string

    For this hobby project I am only using one solar panel but added
    the connection_ok and panel_producing fields just for fun to add some data types
    and replicate something closer to what might be seen in real life.

    Returns the schema definition as a string
    """
    schema_str = """{
                  "$id": "https://github.com/AJTheDataGuy/myURI.schema.json",
                  "$schema": "https://json-schema.org/draft/2020-12/schema#",
                  "additionalProperties": false,
                  "description": "JSON Schema definition for solar panel voltage data",
                  "properties": {
                    "connection_ok": {
                      "description": "Adding a boolean just for fun. Represents whether the panel is connected properly or not.",
                      "type": "boolean"
                    },
                    "panel_producing": {
                      "description": "Defines which solar panel is sending the message (only 1 in this hobby project)",
                      "type": "string"
                    },
                    "voltage_volts": {
                      "description": "Voltage from the panel in volts",
                      "type": "number"
                    }
                  },
                  "title": "SolarPanelVoltageSchema",
                  "type": "object"
                }"""

    return schema_str


def voltage_to_dict(voltage: float, ctx) -> dict:
    """Converts the voltage readings to a dictionary that can be used
    with the JSON Schema

    For this hobby project I am only using one solar panel but added
    the connection_ok and panel_producing fields just for fun to add some data types
    and replicate something closer to what might be seen in real life.

    Returns a dictionary with the voltage reading data
    """

    return {
        "connection_ok": True,
        "panel_producing": "SOLAR_PANEL_1",
        "voltage_volts": voltage,
    }


def produce_to_server(producer, topic: str, key: str, voltage: float, json_serializer):
    """Produces the JSON data to the Confluent Cloud server"""
    producer.produce(
        topic=topic,
        key=key,
        value=json_serializer(voltage, SerializationContext(topic, MessageField.VALUE)),
        on_delivery=delivery_report,
    )


if __name__ == "__main__":
    main()
