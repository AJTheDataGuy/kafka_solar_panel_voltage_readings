"""This module Reads voltage data from a mini solar panel
that is sent to a Kafka broker by a Kafka Producer
(python script) on a Raspberry Pi.

Reads the data as serialized JSON.

This script is intended to be run on a computer that
is NOT the producing Raspberry Pi.

This module reads a JSON serialised message from the producer
and is intended to be paired with the producer script that sends JSON

Consumes three fields from the Kafka JSON:
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

This script is an alternative to the double_deserializer script
and allows for more complex information to be consumed.
"""
import socket
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


def main():
    """main"""
    # Set up Kafka broker and consumer configs
    topic = "solar_panel_voltage_as_json"
    conf = get_consumer_config()
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    schema_str = get_schema_str()

    # Set up deserializer
    json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_panel_reading)
    # Main Loop
    while True:
        try:
            event = consumer.poll(1.0)
            if event is None:
                continue
            reading = json_deserializer(
                event.value(), SerializationContext(topic, MessageField.VALUE)
            )
            raw_timestamp = event.timestamp()
            real_timestamp = datetime.fromtimestamp(raw_timestamp[1] // 1000)
            if reading is not None:
                print(
                    f"Voltage from {reading.panel_producing} at {real_timestamp} is {reading.voltage_volts} volts. Connection OK: {str(reading.connection_ok)} "
                )
        except KeyboardInterrupt:
            break
    consumer.close()


def get_consumer_config() -> dict:
    """Sets the configuration settings for the Kafka consumer

    For my configuration I have chosen to use Confluent Cloud

    Returns the Kafka broker / consumer configuration as a dictionary
    """
    consumer_config = {
        "bootstrap.servers": "REMOVED.australiaeast.azure.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "REMOVED",
        "sasl.password": "REMOVED",
        "client.id": socket.gethostname(),
        "group.id": "solar_panel_voltage_app",
        "auto.offset.reset": "earliest",
    }
    return consumer_config


def get_schema_str() -> str:
    """Defines the schema that the consumed JSON should adhere to

    For my configuration I have chosen to use Confluent Cloud.
    The schema should be defined on the topic usng Confluent Cloud
    before using this function.
    Using Confluent Cloud will also help create this string.

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


class VoltageReader:
    """Helper class for converting the raw JSON from Kafka into
    values that are easier to read and work with"""

    def __init__(self, connection_ok: bool, panel_producing: str, voltage_volts: float):
        """Sets the inputs to attributes on the instance"""
        self.connection_ok = connection_ok
        self.panel_producing = panel_producing
        self.voltage_volts = voltage_volts


def dict_to_panel_reading(raw_json: dict, ctx) -> VoltageReader:
    """Helper function for converting the raw JSON from Kafka to easily read values

    Returns an instance of the VoltageReader class with the data from the JSON
    set to attributes on the instance
    """
    return VoltageReader(
        connection_ok=raw_json["connection_ok"],
        panel_producing=raw_json["panel_producing"],
        voltage_volts=raw_json["voltage_volts"],
    )


if __name__ == "__main__":
    main()
