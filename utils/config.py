import os
from dotenv import load_dotenv
from crewai import LLM

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

    @staticmethod
    def get_llm_params():
        # LLM parameters for deterministic responses
        return {
            "temperature": 0.0,  # Zero temperature for maximum determinism
            "max_tokens": 4000,  # Control output length for consistency
            "top_p": 0.05,  # Very low top_p for highly focused, predictable responses
            "frequency_penalty": 0.0,  # No frequency penalty for consistent terminology
            "presence_penalty": 0.0,  # No presence penalty for consistent structure
        }

    @staticmethod
    def get_llm():
        return LLM(
                model=f"azure/{os.getenv('AZURE_OPENAI_DEPLOYMENT')}",
                api_key=os.getenv("AZURE_API_KEY"),
                api_base=os.getenv("AZURE_API_BASE"),
                api_version=os.getenv("AZURE_API_VERSION"),
                **Config.get_llm_params(),
            )

    @staticmethod
    def get_retry_config():
        return {
            "retry_count": 4
        }