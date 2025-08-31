"""
SQL Schema Analyzer Agent

This module creates a multi-agent system for analyzing SQL Server database schemas.
It uses CrewAI with MSSQL MCP tools to:
1. Extract database schema and table information
3. Validate the table and schema information

The system uses two specialized agents:
- Schema Analyst Agent: Focuses on database structure analysis
- Schema Validator Agent: Focuses on validating the schema analysis
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
DATABASE_NAME = os.getenv("DB_NAME")
MCP_SERVER_PATH = "02-mcp-server/MssqlMcp/bin/Debug/net9.0/MssqlMcp.dll"
MCP_CONNECTION_TIMEOUT = 60

# LLM parameters for deterministic responses
LLM_PARAMS = {
    "temperature": 0.0,  # Zero temperature for maximum determinism
    "max_tokens": 4000,  # Control output length for consistency
    "top_p": 0.05,  # Very low top_p for highly focused, predictable responses
    "frequency_penalty": 0.0,  # No frequency penalty for consistent terminology
    "presence_penalty": 0.0,  # No presence penalty for consistent structure
}

llm_cfg = LLM(
    model=f"azure/{os.getenv('AZURE_OPENAI_DEPLOYMENT')}",
    api_key=os.getenv("AZURE_API_KEY"),
    api_base=os.getenv("AZURE_API_BASE"),
    api_version=os.getenv("AZURE_API_VERSION"),
    **LLM_PARAMS,
)

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


def extract_validation_result(crew_result):
    """Extract validation status from crew execution result.

    Args:
        crew_result: The result from crew.kickoff()

    Returns:
        bool: True if validation passed, False otherwise
    """
    try:
        result_str = str(crew_result)
        if "validation_passed" not in result_str:
            print("⚠️  No validation result found in crew output")
            return False

        # Try to parse as JSON first
        try:
            import json

            result_json = json.loads(result_str)
            return result_json.get("validation_passed", False)
        except json.JSONDecodeError:
            # Fallback to regex parsing
            import re

            validation_match = re.search(
                r'"validation_passed":\s*(true|false)', result_str
            )
            if validation_match:
                return validation_match.group(1).lower() == "true"
            return False

    except Exception as e:
        print(f"⚠️  Error parsing validation result: {e}")
        return False


def execute_analysis_with_retry(crew, max_retries=3):
    """Execute database analysis with retry logic for validation failures.

    Args:
        crew: The CrewAI crew to execute
        max_retries: Maximum number of retry attempts

    Returns:
        tuple: (final_result, validation_passed, attempt_count)
    """
    print(f"\n🚀 Starting analysis with up to {max_retries} attempts...")

    for attempt in range(1, max_retries + 1):
        print(f"\n🔄 Attempt {attempt}/{max_retries}")

        try:
            # Execute the crew
            result = crew.kickoff()

            # Check validation status
            validation_passed = extract_validation_result(result)

            if validation_passed:
                print("✅ Validation PASSED - Schema is accurate!")
                return result, True, attempt
            else:
                print(f"❌ Validation FAILED on attempt {attempt}")
                if attempt < max_retries:
                    print("🔄 Retrying with fresh analysis...")
                else:
                    print(
                        "⚠️  Maximum retries reached. Final results may contain inaccuracies."
                    )

        except Exception as e:
            print(f"❌ Error during attempt {attempt}: {str(e)}")
            if attempt == max_retries:
                raise
            print("🔄 Retrying due to execution error...")

    # If we get here, all retries failed validation
    return result, False, max_retries


def print_execution_summary(result, validation_passed, attempt_count):
    """Print a summary of the execution results.

    Args:
        result: The final crew execution result
        validation_passed: Whether validation ultimately passed
        attempt_count: Number of attempts made
    """
    print("\n" + "=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"🔢 Total attempts: {attempt_count}")
    print(f"✅ Validation status: {'PASSED' if validation_passed else 'FAILED'}")
    print(
        f"🎯 Quality assurance: {'High confidence' if validation_passed else 'Low confidence'}"
    )
    print("=" * 60)
    print("📋 FINAL ANALYSIS RESULTS:")
    print("=" * 60)
    print(result)
    print("=" * 60)


print("🔌 Connecting to MCP Server...")

with MCPServerAdapter(
    mcp_server_parameters, connect_timeout=MCP_CONNECTION_TIMEOUT
) as mcp_tools:
    print("✅ MCP Server connection established")
    
    available_tools = [tool.name for tool in mcp_tools]
    tool_list_str = ", ".join(available_tools)
    print(f"🛠️ Available MCP tools: {tool_list_str}")

    database_analyst_agent = Agent(
        role="Expert Database Analyst",
        goal=f"""Extract all schemas and tables from {DATABASE_NAME}.
        First attempt MUST use the provided MSSQL MCP tools, which are {tool_list_str}.
        If a tool does not return the required info, THEN and ONLY THEN run a custom T-SQL query
        via the MCP query execution tool.
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
        goal=f"""Validate the extracted schemas and tables for {DATABASE_NAME}.
        First attempt MUST use the MSSQL MCP tools, which are {tool_list_str}.
        If information is incomplete, run a custom T-SQL query against INFORMATION_SCHEMA.TABLES
        using the MCP query execution tool.
        Return only JSON in the required format.""",
        backstory=f"""ROLE: SQL Server auditor who verifies schema accuracy.
        WORKFLOW:
        1. Always attempt to use MSSQL MCP tools first ({tool_list_str}).
        2. If they don’t return full details, fallback to INFORMATION_SCHEMA queries via MCP.
        3. Return results ONLY as structured JSON, no extra text. """,
        tools=mcp_tools,
        tools_only=True,
        verbose=True,
        llm=llm_cfg,
    )
    print("✅ Schema Validator Agent created")

    database_analysis_task = Task(
        description=f"""Query {DATABASE_NAME} and return all schemas and table names.

        RULES:
        1. Use these MCP tools first: {tool_list_str}.
        2. If they don’t give the complete list, fallback to a direct T-SQL query against INFORMATION_SCHEMA.TABLES using MCP.
        3. Never assume or fabricate results.
        4. Output ONLY JSON in this format:
        [
        {{ "schema": "schema_name", "table": "table_name" }}
        ] """,
        expected_output="""Strict JSON array of objects with 'schema' and 'table' keys only.""",
        agent=database_analyst_agent,
    )
    print("✅ Schema Analysis Task configured")

    validation_task = Task(
        description=f"""
        Validate the schema and table list extracted from {DATABASE_NAME}.

        RULES:
        1. First, use MCP tools: {tool_list_str}.
        2. If needed, fallback to direct T-SQL queries against INFORMATION_SCHEMA.TABLES via MCP.
        3. Never assume or fabricate issues.
        4. Output ONLY JSON in this format:
        {{
        "validation_passed": true|false,
        "issues": [
            {{
            "type": "missing_foreign_key|invalid_column|duplicate_table|other",
            "table": "TableName",
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

    # Assemble crew
    database_analysis_crew = Crew(
        agents=[database_analyst_agent, validator_agent],
        tasks=[database_analysis_task, validation_task],
        process=Process.sequential,
    )
    print("✅ Database Analysis Crew assembled")
    print(f"👥 Crew members: {len(database_analysis_crew.agents)} agents")
    print(f"📝 Total tasks: {len(database_analysis_crew.tasks)} tasks")

    # Execute with retry
    try:
        final_result, validation_success, total_attempts = execute_analysis_with_retry(
            crew=database_analysis_crew, max_retries=3
        )
        print_execution_summary(final_result, validation_success, total_attempts)

    except Exception as e:
        print(f"\n❌ Fatal error during analysis execution: {str(e)}")
        print("💡 Check that the MCP server is running and database is accessible")
        raise
