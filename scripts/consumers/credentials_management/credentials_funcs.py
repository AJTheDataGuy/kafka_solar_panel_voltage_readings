"""Module to retrieve API keys and secrets
the Microsoft Azure secrets vault.

Requires the key vault name to be set as
a local environment variable name before
this script can be used.
"""
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def retrieve_azure_secret(secret_name:str):
    """Returns a single secret from the Microsoft Azure Key Vault
    Need to use the secret.value attribute to get the actual value
    of the secret

    Steps:
    1. Retrieves the environment variable (must be preset!) for the
        key vault location
    2. Creates a set of credentials (may want to refactor this to be a
        parameter in the future if grabbing many different secrets).
    3. Retrieves the secret from the key vault
    4. Returns the secret
    """

    key_vault_name = os.environ["KEY_VAULT_NAME"]
    key_vault_location = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_location, credential=credential)
    print(f"Retrieving secret {secret_name} from the Azure Key Vault...")
    retrieved_secret = client.get_secret(secret_name)
    print(f"Success! Retrieved {secret_name} from the Azure Key Vault.")
    return retrieved_secret.value