# Purview_Datamap_extractor_Public
 Public code of datamap extraction setup

# Azure Purview Data Extraction Tool

A Python-based tool for extracting metadata from Azure Purview and exporting it to Azure SQL Database.

## 🌟 Features

- Azure Purview catalog search with pagination
- Data processing and SQL Database export
- Database connection testing with 30s stability delay
- Environment variable configuration
- Azure DevOps pipeline support

## 📋 Prerequisites

### Software Requirements
- Python 3.6+
- ODBC Driver 17 for SQL Server

### Azure Resources
  - Purview account
  - SQL Database
  - Service Principal
  - (Optional) Azure DevOps project

### Python Dependencies
pip install azure-purview-catalog azure-identity azure-core pandas pyodbc sqlalchemy urllib3

## 🚀 Installation

1. Clone the repository
git clone <repository-url>
cd <repository-name>

## ⚙️ Configuration

### Azure Purview Setup
Configure your Purview credentials in `PurviewConfig`:

class PurviewConfig:
    tenant_id = "your-tenant-id"
    client_id = "your-client-id"
    client_secret = "your-client-secret"
    purview_endpoint = "your-purview-endpoint"
    purview_scan_endpoint = "your-scan-endpoint"

### Database Setup
Configure your database connection in `DatabaseConfig`:

class DatabaseConfig:
    server = "your-server.database.windows.net"
    database = "your-database"
    username = "your-username"
    password = "your-password"
    table_name = "your-table"

## 💻 Usage

Run the script:
python datamap_extract.py

## 🔧 Code Structure

### PurviewConfig
- Manages Azure Purview authentication
- Stores endpoints and credentials
- Handles secure configuration

### DatabaseConfig
- Manages database connection settings
- Stores SQL Database credentials
- Configures target table information

### PurviewSearchClient
- Performs Purview catalog searches
- Handles authentication
- Manages paginated results
- Processes search data

### DataExporter
- Manages database connections
- Handles data export operations
- Provides error handling for database operations

## 🔄 Version History

- 1.0.0
    - Initial Release
    - Basic functionality implementation
