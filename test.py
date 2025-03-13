import os
from azure.purview.catalog import PurviewCatalogClient
from azure.identity import ClientSecretCredential
from pyapacheatlas.core import PurviewClient
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import AtlasClassification

tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
purview_endpoint = os.getenv("PURVIEW_ENDPOINT")
purview_scan_endpoint = os.getenv("PURVIEW_SCAN_ENDPOINT")




def get_credentials():
	credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
	return credentials

def get_catalog_client():
	credentials = get_credentials()
	client = PurviewCatalogClient(endpoint=purview_endpoint, credential=credentials, logging_enable=True)
	return client

def get_access_token(tenant_id, client_id, client_secret):
    credential = ClientSecretCredential(
        tenant_id=tenant_id, 
        client_id=client_id, 
        client_secret=client_secret
    )
    token = credential.get_token("https://purview.azure.net/.default")
    return token.token
