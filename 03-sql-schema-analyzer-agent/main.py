"""
SQL Schema Analyzer Agent

This module creates a multi-agent system for analyzing SQL Server database schemas.
It uses CrewAI with MSSQL MCP tools to:
1. Extract database schema information (tables, columns, data types)
2. Calculate table statistics (row counts)
3. Provide comprehensive database analysis results

The system uses two specialized agents:
- Schema Analyzer Agent: Focuses on database structure analysis
- Data Statistics Agent: Focuses on data metrics and row counts
"""

from crewai import LLM, Agent, Task, Crew, Process
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters
import os, sys
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import Config

# Load environment variables
load_dotenv()

# Configuration constants
DATABASE_NAME = os.getenv('DB_NAME')
MCP_SERVER_PATH = "02-mcp-server/MssqlMcp/bin/debug/net9.0/MssqlMcp.dll"
MCP_CONNECTION_TIMEOUT = 60

# Azure OpenAI configuration
AZURE_DEPLOYMENT = os.getenv('AZURE_OPENAI_DEPLOYMENT')
AZURE_API_KEY = os.getenv("AZURE_API_KEY")
AZURE_API_BASE = os.getenv("AZURE_API_BASE")
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION")

# LLM parameters for deterministic responses
LLM_PARAMS = {
    "temperature": 0.0,  # Zero temperature for maximum determinism
    "max_tokens": 4000,  # Control output length for consistency
    "top_p": 0.05,  # Very low top_p for highly focused, predictable responses
    "frequency_penalty": 0.0,  # No frequency penalty for consistent terminology
    "presence_penalty": 0.0,  # No presence penalty for consistent structure
}

print("🚀 Starting SQL Schema Analyzer Agent...")
print(f"📊 Target Database: {DATABASE_NAME}")
print(f"🛠️  MCP Server Path: {MCP_SERVER_PATH}")

# Define how to run the local mssql-mcp server
mcp_server_parameters = StdioServerParameters(
    command="dotnet",
    args=[MCP_SERVER_PATH],
    env={
        "CONNECTION_STRING": Config.get_connection_string(include_driver=False),
        **os.environ
    }
)

print("⚙️  MCP Server parameters configured successfully")

print("🔌 Connecting to MCP Server...")

with MCPServerAdapter(mcp_server_parameters, connect_timeout=MCP_CONNECTION_TIMEOUT) as mcp_tools:
    print("✅ MCP Server connection established")
    print(f"🛠️  Available MCP tools: {[tool.name for tool in mcp_tools]}")
    
    # Create specialized agents with descriptive names
    print("\n👤 Creating specialized database analysis agents...")
    
    # Agent for analyzing database schema structure
    schema_analyzer_agent = Agent(
        role="Database Schema Analyst",
        goal=f"Analyze and extract all column information from {DATABASE_NAME} database tables using MSSQL MCP tools only. Never invent or assume data.",
        backstory="Specialized in database schema analysis using MSSQL MCP tools exclusively for accurate data retrieval",
        tools=mcp_tools,
        verbose=True,
        tools_only=True,
        llm=f"azure/{AZURE_DEPLOYMENT}",
        llm_params={
            "api_key": AZURE_API_KEY,
            "api_base": AZURE_API_BASE,
            "api_version": AZURE_API_VERSION,
            **LLM_PARAMS
        },
    )
    print("✅ Schema Analyzer Agent created")

    # Agent for analyzing table row counts and data statistics
    data_statistics_agent = Agent(
        role="Database Statistics Analyst",
        goal=f"Extract accurate row count for each table in {DATABASE_NAME} database tables using MSSQL MCP tools only. Use only real data from the database - never invent, assume, or modify existing schema information.",
        backstory="Specialized in database analysis using MSSQL MCP tools exclusively for accurate data retrieval",
        tools=mcp_tools,
        verbose=True,
        tools_only=True,
        llm=f"azure/{AZURE_DEPLOYMENT}",
        llm_params={
            "api_key": AZURE_API_KEY,
            "api_base": AZURE_API_BASE,
            "api_version": AZURE_API_VERSION,
            **LLM_PARAMS
        },
    )
    print("✅ Data Statistics Agent created")

    # Create specific tasks with clear descriptions
    print("\n📋 Setting up analysis tasks...")
    
    schema_analysis_task = Task(
        description=f"Analyze and list all columns with their data types for all tables in the {DATABASE_NAME} database.",
        expected_output="JSON format",
        agent=schema_analyzer_agent
    )
    print("✅ Schema Analysis Task configured")

    statistics_analysis_task = Task(
        description=f"Extract the row count as row_count for each table in the {DATABASE_NAME} database.",
        expected_output="JSON format with schema and row_count",
        agent=data_statistics_agent,
        context=[schema_analysis_task]  # Explicitly depend on the first task's output
    )
    print("✅ Statistics Analysis Task configured")

    # Create and configure the analysis crew
    print("\n🎯 Assembling database analysis crew...")
    database_analysis_crew = Crew(
        agents=[schema_analyzer_agent, data_statistics_agent],
        tasks=[schema_analysis_task, statistics_analysis_task],
        process=Process.sequential
    )
    print("✅ Database Analysis Crew assembled")
    print(f"👥 Crew members: {len(database_analysis_crew.agents)} agents")
    print(f"📝 Total tasks: {len(database_analysis_crew.tasks)} tasks")

    # Execute the analysis
    print("\n🚀 Starting database analysis execution...")
    print("📋 Task execution order:")
    print("   1️⃣ Schema Analysis Task (extract table structures)")
    print("   2️⃣ Statistics Analysis Task (add row counts to schema data)")
    print("   ⚠️  Note: Task 2 depends on Task 1's output to prevent hallucinations")
    
    try:
        analysis_result = database_analysis_crew.kickoff()
        print("\n✅ Database analysis completed successfully!")
        print("=" * 60)
        print("📊 FINAL ANALYSIS RESULTS:")
        print("=" * 60)
        print(analysis_result)
        print("=" * 60)
        print("✅ Analysis complete - Row counts should now be accurate!")
    except Exception as e:
        print(f"\n❌ Error during analysis execution: {str(e)}")
        print("💡 Check that the MCP server is running and database is accessible")
        raise

