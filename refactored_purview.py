from azure.purview.catalog import PurviewCatalogClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import urllib

class PurviewConfig:
    def __init__(self):
        self.tenant_id = "Azure TENANT ID"
        self.client_id = "Azure CLIENT ID"
        self.client_secret = "Azure CLIENT SECRET"
        self.purview_endpoint = "https://"+ purview_account_name + ".purview.azure.com/"
        self.purview_scan_endpoint = "https://"+ purview_account_name +".purview.azure.com/"

class DatabaseConfig:
    def __init__(self):
        self.server = 'Azure SQL SERVER NAME'
        self.database = 'Azure SQL DATABASE NAME'
        self.username = 'Azure SQL USERNAME'
        self.password = 'Azure SQL PASSWORD'
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.table_name = 'Azure SQL TABLE NAME'

class PurviewSearchClient:
    def __init__(self, config: PurviewConfig):
        self.config = config
        self.credentials = self._get_credentials()
        self.catalog_client = self._get_catalog_client()
        
    def _get_credentials(self):
        return ClientSecretCredential(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            tenant_id=self.config.tenant_id
        )
    
    def _get_catalog_client(self):
        return PurviewCatalogClient(
            endpoint=self.config.purview_endpoint,
            credential=self.credentials,
            logging_enable=True
        )
    
    def search(self, keywords="*", limit=1000):
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
            search_counter = response["@search.count"]
            offset_counter = search_counter
            
            while True:
                response = self.catalog_client.discovery.query(search_request=body_input)
                df = pd.DataFrame(response)
                df_list.append(df)
                
                if offset_counter == 0:
                    break
                    
                if offset_counter >= 1000:
                    offset_counter -= 1000
                    search_counter -= 1000
                
                if offset_counter <= 1000 and limit == 1000:
                    limit = offset_counter
                    offset_counter -= limit
                
                body_input.update({
                    "limit": limit,
                    "offset": offset_counter
                })
                
            return pd.concat(df_list)
            
        except HttpResponseError as e:
            print(f"Search error: {e}")
            return None

class DataExporter:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.engine = self._create_engine()
    
    def _create_engine(self):
        connection_string = (
            f"mssql+pyodbc://{self.db_config.username}:{urllib.parse.quote_plus(self.db_config.password)}"
            f"@{self.db_config.server}:1433/{self.db_config.database}"
            f"?driver={self.db_config.driver.replace(' ', '+')}"
        )
        return create_engine(connection_string)
    
    def export_to_sql(self, df, table_name=None):
        if table_name is None:
            table_name = self.db_config.table_name
            
        try:
            df.to_sql(
                table_name,
                con=self.engine,
                if_exists='append',
                index=False
            )
            print(f"DataFrame successfully appended to the '{table_name}' table in Azure SQL Database.")
        except Exception as e:
            print(f"Export error: {e}")

def main():
    # Initialize configurations
    purview_config = PurviewConfig()
    db_config = DatabaseConfig()
    
    # Create clients
    purview_client = PurviewSearchClient(purview_config)
    data_exporter = DataExporter(db_config)
    
    # Perform search
    search_results = purview_client.search()
    
    if search_results is not None:
        # Process results
        jdf = pd.json_normalize(search_results.value)
        jdf['date'] = datetime.now().date()
        
        # Export to SQL
        data_exporter.export_to_sql(jdf)

if __name__ == "__main__":
    main()