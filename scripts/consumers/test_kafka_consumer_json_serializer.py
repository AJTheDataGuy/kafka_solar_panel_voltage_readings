"""Tests for the kafka_consumer_json_serializer module

Module is intended to be run from the command line
with the pytest module

It is recommended to run this file with the command
python -m pytest tests_kafka_producer_json_serializer.py to run properly

Due to python's odd relative import system running this file may not
work without the -m or if using pytest directly without the "python -m" prefix.

Originally I had a full pipeline integration test including consuming from the
Confluent Cloud server but the test kept timing out despite having similiar
code to the actual script being tested (and which consumed the server successfully).
I have thus removed that test for now. 
"""
# Standard Library Imports
import json

# 3rd Party Imports
from azure.core.exceptions import ClientAuthenticationError,HttpResponseError
import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import pytest

# Custom Module Imports
from credentials_management.credentials_funcs import retrieve_azure_secret


import kafka_consumer_json_deserializer
from kafka_consumer_json_deserializer import (
    get_schema_str,
    get_consumer_config,
    dict_to_panel_reading,
    consume_voltage_reading,
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
                             'confluent-cloud-server-api-secret']:
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


def test_validate_consumer_config():
    """Validates the consumer configuration"""
    config = get_consumer_config()
    try:
        consumer = Consumer(config)
        consumer.close()
    except KafkaException:
        pytest.fail("Consumer configuration is not valid")


def test_valid_json_deserialization():
    """Validates that the dictionary definition for the deserializer
    matches the schema string
    """
    schema_str = get_schema_str()
    try:
        json_deserializer = JSONDeserializer(
            schema_str, from_dict=dict_to_panel_reading
        )
    except KafkaException:
        pytest.fail(
            "Error with deserializer. Please check that definition matches in schema string and dictionary function"
        )