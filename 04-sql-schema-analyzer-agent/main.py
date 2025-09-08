"""
SQL Schema Analyzer Agent

This module implements a multi-agent system for analyzing SQL Server database schemas.
It uses CrewAI with MSSQL MCP tools to:

1. Extract column information from the database
2. Validate the extracted column information against the database

Agents:
- **Database Analyst Agent**: Extracts column information.
- **Schema Validator Agent**: Validates extracted column information for correctness and completeness.
"""

from crewai import LLM, Agent, Task, Crew, Process
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters
import os, sys, json, re
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import Config
from utils.crew_utils import execute_analysis_with_retry, print_execution_summary

# Load environment variables
load_dotenv()

# Configuration constants
DATABASE_NAME = os.getenv("DB_NAME")
MCP_SERVER_PATH = "02-mcp-server/MssqlMcp/bin/Debug/net9.0/MssqlMcp.dll"
MCP_CONNECTION_TIMEOUT = 60
llm_cfg = Config.get_llm()

print("🚀 Starting SQL Schema Analyzer Agent...")
print(f"📊 Target Database: {DATABASE_NAME}")
print(f"🛠️ MCP Server Path: {MCP_SERVER_PATH}")

# Define how to run the local mssql-mcp server
mcp_server_parameters = StdioServerParameters(
    command="dotnet",
    args=[MCP_SERVER_PATH],
    env={
        "CONNECTION_STRING": Config.get_connection_string(include_driver=False),
        **os.environ,
    },
)

print("⚙️ MCP Server parameters configured successfully")
print("🔌 Connecting to MCP Server...")

tables = json.load(open("output/tables.json"))
for table in tables:
    table_name = table['table']
    print(f"🗂️ Processing table: {table_name}")

    with MCPServerAdapter(
        mcp_server_parameters, connect_timeout=MCP_CONNECTION_TIMEOUT
    ) as mcp_tools:
        print("✅ MCP Server connection established")

        available_tools = [tool.name for tool in mcp_tools]
        tool_list_str = ", ".join(available_tools)
        print(f"🛠️ Available MCP tools: {tool_list_str}")

        database_analyst_agent = Agent(
            role="Expert Database Analyst",
            goal=f"""Extract column information such as column_name, data_type, length, is_primary_key, is_nullable of {table_name} table from {DATABASE_NAME} DB.
            First attempt MUST use the provided MSSQL MCP tools, which are {tool_list_str}.
            If a tool does not return the required info, THEN and ONLY THEN run a custom T-SQL query using sys.tables, sys.columns, sys.types, sys.indexes and sys.index_columns via the MCP query execution tool.
            Output strictly as JSON (no explanations, no comments). """,
            backstory=f"""ROLE: Expert SQL Server schema extractor.
            WORKFLOW:
            1. Always attempt to use MSSQL MCP tools first ({tool_list_str}).
            2. If the tool result is incomplete, fallback to executing a direct T-SQL query through MCP.
            3. Never invent or assume missing details.
            4. Always return only JSON in the required schema. """,
            tools=mcp_tools,
            verbose=True,
            tools_only=True,
            llm=llm_cfg,
        )
        print("✅ Database Analyst Agent created")

        validator_agent = Agent(
            role="Expert Database Validator",
            goal=f"""Validate the extracted column information of each column of {table_name} table from {DATABASE_NAME} DB.
            First attempt MUST use the MSSQL MCP tools, which are {tool_list_str}.
            If a tool does not return the required info, THEN and ONLY THEN run a custom T-SQL query (sys.tables, sys.columns, sys.types, sys.indexes and sys.index_columns) via the MCP query execution tool.
            Return only JSON in the required format.""",
            backstory=f"""ROLE: SQL Server auditor.
            WORKFLOW:
            1. Always attempt to use MSSQL MCP tools first ({tool_list_str}).
            2. If they don’t return full details, fallback to executing a direct T-SQL query through MCP.
            3. Never invent or assume missing details.
            4. Return results ONLY as structured JSON, no extra text. """,
            tools=mcp_tools,
            tools_only=True,
            verbose=True,
            llm=llm_cfg,
        )
        print("✅ Schema Validator Agent created")

        database_analysis_task = Task(
            description=f"""Query {table_name} table of {DATABASE_NAME} DB and return column information such as column_name, data_type, length, is_primary_key, is_nullable.

            RULES:
            1. First, use MCP tools: {tool_list_str}.
            2. If they don’t give the complete list, fallback to a direct T-SQL query against sys.tables, sys.columns, sys.types, sys.indexes and sys.index_columns using MCP.
            3. Never assume or fabricate results.
            4. Output ONLY JSON in this format:
            [
                {{  
                    "table_name": "table_name", 
                    "column_name": "column_name", 
                    "data_type": "data_type", 
                    "length": "length", 
                    "is_primary_key": "is_primary_key", 
                    "is_nullable": "is_nullable" 
                }}
            ] """,
            expected_output="""Strict JSON array of objects with 'table_name', 'column_name', 'data_type', 'length', 'is_primary_key', 'is_nullable' keys only.""",
            agent=database_analyst_agent,
        )
        print("✅ Schema Analysis Task configured")

        validation_task = Task(
            description=f"""Validate the extracted column information of each column of {table_name} table from {DATABASE_NAME} DB.

            RULES:
            1. First, use MCP tools: {tool_list_str}.
            2. If they don’t give the complete list, fallback to direct T-SQL queries against sys.tables, sys.columns, sys.types, sys.indexes and sys.index_columns using MCP.
            3. Never assume or fabricate issues.
            4. Output ONLY JSON in this format:
            {{
                "validation_passed": true|false,
                "issues": [
                    {{
                    "type": "missing_type|incorrect_type|invalid_type|non-existing_type|duplicate_type|others", // type will be column, type, index, index_column, primary_key, nullable
                    "name": "name", // name will be column_name, type_name, index_name, index_column_name, primary_key_name, nullable_name
                    "details": "Explanation of the issue"
                    }}
                ],
                "message": "<summary string>"
            }}
            """,
            expected_output="Strict JSON object with validation_passed, issues[], and message.",
            agent=validator_agent,
            context=[database_analysis_task],
        )

        print("✅ Schema Validation Task configured")

        # Crew = Analyst + Validator running sequentially
        database_analysis_crew = Crew(
            agents=[database_analyst_agent, validator_agent],
            tasks=[database_analysis_task, validation_task],
            process=Process.sequential,
        )
        print("✅ Database Analysis Crew assembled")
        print(f"👥 Crew members: {len(database_analysis_crew.agents)} agents")
        print(f"📝 Total tasks: {len(database_analysis_crew.tasks)} tasks")

        try:
            validation_result, analyst_result, validation_success, total_attempts = execute_analysis_with_retry(crew=database_analysis_crew, max_retries=4)

            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{table_name}_schema.json")

            if validation_success and analyst_result:
                with open(output_file, "w") as f:
                    tables_json = json.loads(analyst_result)
                    json.dump(tables_json, f, indent=2)
                print(f"💾 Saved extracted column information to {output_file}")
            else:
                print(f"⚠️ Validation failed hence no {output_file} saved.")

            print_execution_summary(
                validation_result, analyst_result, validation_success, total_attempts
            )

        except Exception as e:
            print(f"\n❌ Fatal error during analysis execution: {str(e)}")
            print("💡 Check that the MCP server is running and database is accessible")
            raise
