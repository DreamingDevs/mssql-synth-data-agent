import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    DB_SERVER = os.getenv("DB_SERVER") 
    DB_NAME = os.getenv("DB_NAME")
    DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    @staticmethod
    def get_connection_string(include_driver=True):
        server = f"tcp:{Config.DB_SERVER},1433"
        connection_parts = []
        
        # Add driver only if explicitly requested and available
        if include_driver and Config.DB_DRIVER:
            connection_parts.append(f"DRIVER={{{Config.DB_DRIVER}}}")
        
        # Add other required parts
        connection_parts.extend([
            f"SERVER={server}",
            f"DATABASE={Config.DB_NAME}",
            "Encrypt=yes",
            "TrustServerCertificate=yes",
            "Connection Timeout=30",
            f"UID={Config.DB_USER}",
            f"PWD={Config.DB_PASSWORD}"
        ])
        
        return ";".join(connection_parts) + ";"

    @staticmethod
    def get_master_connection_string(include_driver=True):
        """Get connection string for master database (for database creation)"""
        server = f"tcp:{Config.DB_SERVER},1433"
        connection_parts = []
        
        # Add driver only if explicitly requested and available
        if include_driver and Config.DB_DRIVER:
            connection_parts.append(f"DRIVER={{{Config.DB_DRIVER}}}")
        
        # Add other required parts (with master database)
        connection_parts.extend([
            f"SERVER={server}",
            "DATABASE=master",
            "Encrypt=yes",
            "TrustServerCertificate=yes",
            "Connection Timeout=30",
            f"UID={Config.DB_USER}",
            f"PWD={Config.DB_PASSWORD}"
        ])
        
        return ";".join(connection_parts) + ";"