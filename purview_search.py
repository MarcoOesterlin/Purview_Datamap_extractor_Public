from azure.purview.catalog import PurviewCatalogClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
import pandas as pd
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import urllib
import msal

date = datetime.now().date()
export_csv_path = "purview_search_export_.csv"


keywords = "*"
tenant_id = "TENANT ID"
client_id = "CLIENT ID"
client_secret = "CLIENT SECRET"
purview_endpoint = "https://" + Purview_account_name +".purview.azure.com/"
purview_scan_endpoint = "https://" + Purview_account_name +"purview.azure.com/"

def get_credentials():
	credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
	return credentials

def get_catalog_client():
	credentials = get_credentials()
	client = PurviewCatalogClient(endpoint=purview_endpoint, credential=credentials, logging_enable=True)
	return client


offset_counter = 0
search_counter = 0
df_list = []
limit = 1000

body_input={
	"keywords": keywords,
	"limit": limit,
	"offset": offset_counter,
}

try:
	catalog_client = get_catalog_client()
except ValueError as e:
	print(e)


try:
	response = catalog_client.discovery.query(search_request=body_input)
	search_counter = response["@search.count"]
	print(search_counter)
	offset_counter = search_counter

	while True:
		response = catalog_client.discovery.query(search_request=body_input)
		df = pd.DataFrame(response)
		res = df
		df_list.append(res)
		print(response)

		if offset_counter == 0:
			break	

		if offset_counter >= 1000:
			offset_counter = offset_counter - 1000
			search_counter = search_counter - 1000

		if offset_counter <= 1000 and limit == 1000:
			limit = offset_counter
			offset_counter = offset_counter - limit
			
		
		body_input={ 
			"keywords": keywords,
			"limit": limit,
			"offset": offset_counter,
			}
		
except HttpResponseError as e:
	print(e)




df_res = pd.concat(df_list)
jdf = pd.json_normalize(df_res.value)
jdf['date'] = date

#jdf.to_csv(export_csv_path, index=False)	

#jdf_sql = jdf.to_sql(method = 'multi')

# Azure SQL Database connection details
server = 'Azure SQL SERVER'  # Replace with your server name
database = 'Azure SQL DB'                  # Replace with your database name
username = 'AZURE SQL DB USERNAME'                       # Replace with your username
password = 'AZURE SQL DB USERNAME PASSWORD'                       # Replace with your password
driver = 'ODBC Driver 17 for SQL Server'

# Create the connection string
connection_string = (
    f"mssql+pyodbc://{username}:{urllib.parse.quote_plus(password)}@{server}:1433/"
    f"{database}?driver={driver.replace(' ', '+')}"
)

# Create a database engine
engine = create_engine(connection_string)

# Define table name
table_name = 'AZURE SQL TABLE NAME'  # Replace with your desired table name

# Append DataFrame to Azure SQL table with batching
try:
    jdf.to_sql(
        table_name,
        con=engine,
        if_exists='append',
        index=False,
        #method='multi'  # Use multi-row INSERT statements
    )
    print(f"DataFrame successfully appended to the '{table_name}' table in Azure SQL Database.")
except Exception as e:
    print(f"Error: {e}")



