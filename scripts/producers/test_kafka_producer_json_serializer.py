"""Tests for the kafka_producer_json_serializer module

Module is intended to be run from the command line
with the pytest module

It is recommended to run this file with the command
python -m pytest tests_kafka_producer_json_serializer.py to run properly

Due to python's odd relative import system running this file may not
work without the -m or if using pytest directly without the "python -m" prefix.

Originally I had a full pipeline integration test including producing to the
Confluent Cloud server but the test kept resulting in false passes even with faulty
connection details. My initial theory was that it was timing out, passing over
the producer function with no error and resulting in a pass for pytest.
But even after I added a timer with the time module and a pytest fail condition
on not being able to produce to the server after 30 seconds the test would still pass.
I have thus removed that test for now.
"""
# Standard Library Imports
import json
import struct

# 3rd Party Imports
from azure.core.exceptions import ClientAuthenticationError,HttpResponseError

from confluent_kafka import Producer
from confluent_kafka import KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
import pytest

# Custom Module Imports
from credentials_management.credentials_funcs import retrieve_azure_secret
import kafka_producer_json_serializer
from kafka_producer_json_serializer import (
    get_schema_str,
    get_producer_config,
    get_schema_registry_config,
    voltage_to_dict,
    produce_to_server,
)
def test_azure_key_vault_connection():
    """Validates that the Azure Key Vault can be reached
    by testing that at least 1 key can be retrieved"""
    try:
        retrieve_azure_secret('confluent-cloud-bootstrap-server-name')
    except ClientAuthenticationError:
        pytest.fail('Error connecting to Azure Key Vault. Please check the environment variables are configured correctly.')

def test_all_azure_key_vault_keys():
    """Tests that all the keys in the Azure Key Vault for this
        Confluent Kafka program scope
        have valid names. I.e. checks that none of the keys
        in the key vault have had their names changed
        
        Fails on any key change.
        """
    for azure_secret in ['confluent-cloud-bootstrap-server-name',
                             'confluent-bootstrap-server-api-key',
                             'confluent-cloud-server-api-secret',
                             'confluent-cloud-schema-registry-server-name',
                             'confluent-cloud-schema-api-key',
                             'confluent-cloud-schema-api-secret']:
        try:
            retrieve_azure_secret(azure_secret)
        except HttpResponseError:
            pytest.fail(f'Error retrieving Azure Key Vault key {azure_secret}. Please check that the name of the secret has not changed.')

def test_schema_str_valid_json():
    """Tests that the schema string is valid JSON"""
    schema_str = get_schema_str()
    try:
        json.loads(schema_str)
    except json.decoder.JSONDecodeError:
        pytest.fail("Error: schema string is not valid JSON")


def test_validate_producer_config():
    """Validates the producer configuration"""
    config = get_producer_config()
    producer = Producer(config)
    try:
        Producer(config)
        producer.flush()
    except KafkaException:
        pytest.fail("Producer configuration is not valid")


def test_validate_schema_registry_config():
    """Validates the schema registry configuration"""
    schema_registry_config = get_schema_registry_config()
    try:
        SchemaRegistryClient(schema_registry_config)
    except KafkaException:
        pytest.fail("Schema registry configuration is not valid")


def test_valid_json_serialization():
    """Tests that JSON serialization is as intended and that the dictionary
    definition provided matches what is in the schema string."""
    schema_registry_config = get_schema_registry_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    schema_str = get_schema_str()

    # Set up Serializer
    try:
        json_serializer = JSONSerializer(
            schema_str, schema_registry_client, voltage_to_dict
        )
    except KafkaException:
        pytest.fail(
            "Error with serialization context. Please check the schema definition in the dictionary and schema string"
        )