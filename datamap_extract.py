from azure.purview.catalog import PurviewCatalogClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import urllib

class PurviewConfig:
    """Configuration class for Azure Purview authentication and endpoints.
    
    Stores the necessary credentials and endpoints for connecting to Azure Purview,
    including tenant ID, client ID, client secret, and Purview endpoints.
    """
    def __init__(self):
        self.tenant_id = "Azure TENANT ID"
        self.client_id = "Azure CLIENT ID"
        self.client_secret = "Azure CLIENT SECRET"
        self.purview_endpoint = "https://"+ purview_account_name + ".purview.azure.com/"
        self.purview_scan_endpoint = "https://"+ purview_account_name +".purview.azure.com/"


class DatabaseConfig:
    """Configuration class for Azure SQL Database connection settings.
    
    Stores the necessary credentials and connection details for Azure SQL Database,
    including server name, database name, authentication credentials, and target table.
    """
    def __init__(self):
        self.server = 'Azure SQL SERVER NAME'
        self.database = 'Azure SQL DATABASE NAME'
        self.username = 'Azure SQL USERNAME'
        self.password = 'Azure SQL PASSWORD'
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.table_name = 'Azure SQL TABLE NAME'

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

class DataExporter:
    """Handles data export operations to Azure SQL Database.
    
    Manages the connection and data export operations to Azure SQL Database using SQLAlchemy.
    
    Args:
        db_config (DatabaseConfig): Configuration object containing database connection details.
    """
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine for database operations.
        
        Returns:
            Engine: SQLAlchemy engine instance for database connections.
        """
        connection_string = (
            f"mssql+pyodbc://{self.db_config.username}:{urllib.parse.quote_plus(self.db_config.password)}"
            f"@{self.db_config.server}:1433/{self.db_config.database}"
            f"?driver={self.db_config.driver.replace(' ', '+')}"
        )
        return create_engine(connection_string)
    
    def export_to_sql(self, df, table_name=None):
        """Export DataFrame to Azure SQL Database.
        
        Args:
            df (pandas.DataFrame): DataFrame to export.
            table_name (str, optional): Target table name. Defaults to configured table name.
        """
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
    """Main execution function for the data extraction and export process.
    
    Orchestrates the entire workflow:
    1. Initializes Purview and database configurations
    2. Creates necessary client instances
    3. Performs Purview catalog search
    4. Processes search results into a DataFrame
    5. Exports processed data to Azure SQL Database
    """
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