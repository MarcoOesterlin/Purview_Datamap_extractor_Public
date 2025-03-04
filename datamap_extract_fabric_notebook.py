from azure.purview.catalog import PurviewCatalogClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import datetime
import uuid
import os
import notebookutils

class PurviewConfig:
    """Configuration class for Azure Purview authentication and endpoints.

    
    Stores the necessary credentials and endpoints for connecting to Azure Purview,
    including tenant ID, client ID, client secret, and Purview endpoints.
    """
    def __init__(self):
        self.tenant_id = notebookutils.credentials.getSecret("KEYVAULT_URL", "TENANTID")
        self.client_id = notebookutils.credentials.getSecret("KEYVAULT_URL", "CLIENTID")
        self.client_secret = notebookutils.credentials.getSecret("KEYVAULT_URL", "CLIENTSECRET")
        self.purview_endpoint = notebookutils.credentials.getSecret("KEYVAULT_URL", "PURVIEWENDPOINT")
        self.purview_scan_endpoint = notebookutils.credentials.getSecret("KEYVAULT_URL", "PURVIEWSCANENDPOINT")

class PurviewSearchClient:
    """Client for performing searches in Azure Purview.
    
    Handles authentication and search operations against the Azure Purview catalog,
    with support for paginated results retrieval.
    
    Args:
        config (PurviewConfig): Configuration object containing Purview credentials and endpoints.
    """
    def __init__(self, config: PurviewConfig):
        self.config = config
        self.credentials = self._get_credentials()
        self.catalog_client = self._get_catalog_client()
        
    def _get_credentials(self):
        """Create Azure client credentials object.
        
        Returns:
            ClientSecretCredential: Authenticated credentials for Azure services.
        """
        return ClientSecretCredential(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            tenant_id=self.config.tenant_id
        )
    
    def _get_catalog_client(self):
        """Initialize the Purview catalog client.
        
        Returns:
            PurviewCatalogClient: Authenticated client for Purview catalog operations.
        """
        return PurviewCatalogClient(
            endpoint=self.config.purview_endpoint,
            credential=self.credentials,
            logging_enable=True
        )
    
    def search(self, keywords="*", limit=1000):
        """Search the Purview catalog with pagination support.
        
        Args:
            keywords (str, optional): Search keywords. Defaults to "*" for all results.
            limit (int, optional): Number of records per page. Defaults to 1000.
            
        Returns:
            pandas.DataFrame: Combined search results as a DataFrame, or None if search fails.
        """
        df_list = []
        offset_counter = 0
        
        try:
            body_input = {
                "keywords": keywords,
                "limit": limit,
                "offset": offset_counter,
            }
            
            # Get initial count
            response = self.catalog_client.discovery.query(search_request=body_input)
            total_records = response["@search.count"]
            offset_counter = 0  # Start from beginning
            
            while offset_counter < total_records:
                body_input.update({
                    "limit": limit,
                    "offset": offset_counter
                })
                
                response = self.catalog_client.discovery.query(search_request=body_input)
                df = pd.DataFrame(response)
                df_list.append(df)
                print(f"Retrieved records {offset_counter} to {min(offset_counter + limit, total_records)}")
                
                offset_counter += limit
            
            return pd.concat(df_list)
            
        except HttpResponseError as e:
            print(f"Search error: {e}")
            return None

def main():
    """Main execution function for the data extraction and JSON export process.
    
    Orchestrates the workflow:
    1. Initializes Purview configuration
    2. Creates necessary client instance
    3. Performs Purview catalog search
    4. Processes search results into a DataFrame
    5. Exports processed data to JSON file
    """

    now = datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    run_id = int(now.strftime("%H%M%S%f"))

    # Initialize configurations
    purview_config = PurviewConfig()
    
    # Create client
    purview_client = PurviewSearchClient(purview_config)
    
    # Perform search
    search_results = purview_client.search()
    
    if search_results is not None:
        # Process results
        jdf = pd.json_normalize(search_results.value)
        
        # Convert all dictionary/object columns to strings
        for column in jdf.columns:
            if jdf[column].dtype == 'object':
                jdf[column] = jdf[column].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)

        # Create directory path
        path = f"/lakehouse/default/Files/data/load_type=full/year={year}/month={month}/day={day}/run_id={run_id}"
        os.makedirs(path, exist_ok=True)

        # Generate unique filename with .json extension
        output_filename = f"{str(uuid.uuid4())}.json"
        output_filepath = os.path.join(path, output_filename)

        try:
            # Convert DataFrame directly to JSON using to_json
            jdf.to_json(output_filepath, orient='records', indent=2, date_format='iso')
            print(f"Data successfully exported to {output_filepath}")
        except Exception as e:
            print(f"Export error: {e}")

if __name__ == "__main__":
    main()