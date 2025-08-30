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
MCP_SERVER_PATH = "02-mcp-server/MssqlMcp/bin/Debug/net9.0/MssqlMcp.dll"
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
        goal=f"Extract schema information from {DATABASE_NAME} database tables including detailed column properties and foreign key relationships using MSSQL MCP tools ONLY. ",
        backstory="Specialized in comprehensive database schema and metadata extraction using MSSQL MCP tools ONLY. Never invent or assume data. ",
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
        goal=f"Extract the number of rows as row_count for all {DATABASE_NAME} database tables using MSSQL MCP tools ONLY. ",
        backstory="Specialized in querying database for accurate row counts using MSSQL MCP tools ONLY. Never invent or assume data. ",
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

    # Agent for validating row counts against actual database values
    row_count_validator_agent = Agent(
        role="Database Row Count Validator",
        goal=f"Validate row count accuracy by comparing reported counts against actual database values for {DATABASE_NAME} tables using MSSQL MCP tools ONLY.",
        backstory="Specialized in data validation and accuracy verification using MSSQL MCP tools to ensure row count integrity. Never accepts unverified data.",
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
    print("✅ Row Count Validator Agent created")

    # Create specific tasks with clear descriptions
    print("\n📋 Setting up analysis tasks...")
    
    schema_analysis_task = Task(
        description=f"""Extract schema information for all tables in the {DATABASE_NAME} database. For each table, retrieve:
        1. Table schema and name
        2. All columns with properties: name, data type, length, precision, scale, nullable
        3. All foreign key relationships with constraint names, columns, referenced tables and columns""",
        expected_output="""JSON format with this exact structure:
        {
          "tables": [
            {
              "schema": "SchemaName",
              "name": "TableName",
              "columns": [
                {
                  "name": "ColumnName",
                  "type": "data_type",
                  "length": 4,
                  "precision": 10,
                  "scale": 0,
                  "nullable": true|false
                }
              ],
              "foreignKeys": [
                {
                  "name": "FK_ConstraintName",
                  "column": "LocalColumn",
                  "referencedTable": "ReferencedTable",
                  "referencedColumn": "ReferencedColumn"
                }
              ]
            }
          ]
        }""",
        agent=schema_analyzer_agent
    )
    print("✅ Schema Analysis Task configured")

    statistics_analysis_task = Task(
        description=f"Extract the number of rows as row_count for each table in the {DATABASE_NAME} database. ",
        expected_output="""Take the previous task's JSON and add row_count field to each table:
        [{
            "name": "tableName",
            "row_count": 123
        }]""",
        agent=data_statistics_agent,
        context=[schema_analysis_task]  # Explicitly depend on the first task's output
    )
    print("✅ Statistics Analysis Task configured")

    row_count_validation_task = Task(
        description=f"""Validate the accuracy of row counts from the previous statistics task by:
        1. Taking the table names and reported row_count values from the previous task
        2. For each table, execute ReadData tool with SQL: "SELECT COUNT(*) as actual_count FROM [schema].[table_name]"
        3. Compare the reported row_count with the actual_count from the database
        4. Return true if ALL row counts are accurate, false if ANY are inaccurate.""",
        expected_output="""Simple boolean result:
        {
          "validation_passed": true|false,
          "message": "All row counts are accurate" or "Some row counts are inaccurate"
        }""",
        agent=row_count_validator_agent,
        context=[statistics_analysis_task]  # Explicitly depend on the statistics task's output
    )
    print("✅ Row Count Validation Task configured")

    # Create and configure the analysis crew
    print("\n🎯 Assembling database analysis crew...")
    database_analysis_crew = Crew(
        agents=[schema_analyzer_agent, data_statistics_agent, row_count_validator_agent],
        tasks=[schema_analysis_task, statistics_analysis_task, row_count_validation_task],
        process=Process.sequential
    )
    print("✅ Database Analysis Crew assembled")
    print(f"👥 Crew members: {len(database_analysis_crew.agents)} agents")
    print(f"📝 Total tasks: {len(database_analysis_crew.tasks)} tasks")

    # Execute the analysis with retry loop
    print("\n🚀 Starting database analysis execution with validation loop...")
    print("📋 Task execution order:")
    print("   1️⃣ Schema Analysis Task (extract complete table structures)")
    print("   2️⃣ Statistics Analysis Task (extract row counts for each table)")
    print("   3️⃣ Row Count Validation Task (validate reported vs actual counts)")
    print("   🔄 If validation fails, process will retry from step 2")
    print("   🔍 Validation uses direct SELECT COUNT(*) queries for verification")
    
    max_retries = 3
    retry_count = 0
    validation_passed = False
    
    try:
        while not validation_passed and retry_count < max_retries:
            retry_count += 1
            print(f"\n🔄 Attempt {retry_count}/{max_retries}")
            
            # Execute the crew
            analysis_result = database_analysis_crew.kickoff()
            
            # Check if validation passed by examining the result
            if "validation_passed" in str(analysis_result):
                import json
                
                # Extract the validation result from the final output
                try:
                    # Look for validation_passed in the output
                    result_json = json.loads(str(analysis_result))
                    validation_passed = result_json.get("validation_passed", False)
                    
                    if validation_passed:
                        print("\n✅ Validation PASSED - All row counts are accurate!")
                        break
                    else:
                        print(f"\n❌ Validation FAILED - Retrying... (Attempt {retry_count}/{max_retries})")
                        if retry_count < max_retries:
                            print("🔄 Restarting statistics analysis task...")
                        else:
                            print("⚠️  Maximum retries reached. Final results may contain inaccuracies.")
                except Exception as parse_error:
                    print(f"⚠️  Could not parse validation result: {parse_error}")
                    validation_passed = False  # Assume failed if can't parse
            else:
                print("⚠️  No validation result found in output")
                validation_passed = False  # Assume failed if no validation found
        
        print("\n✅ Database analysis process completed!")
        print("=" * 60)
        print("📊 FINAL ANALYSIS RESULTS:")
        print("=" * 60)
        print(analysis_result)
        print("=" * 60)
        print(f"🔢 Total attempts: {retry_count}")
        print(f"✅ Validation status: {'PASSED' if validation_passed else 'FAILED'}")
        
    except Exception as e:
        print(f"\n❌ Error during analysis execution: {str(e)}")
        print("💡 Check that the MCP server is running and database is accessible")
        raise

