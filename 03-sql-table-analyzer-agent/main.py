"""
SQL Schema Analyzer Agent

This module implements a multi-agent system for analyzing SQL Server database schemas.
It uses CrewAI with MSSQL MCP tools to:

1. Extract database schema and table information
2. Validate the extracted schema information against the database

Agents:
- **Database Analyst Agent**: Extracts schema and table names.
- **Schema Validator Agent**: Validates extracted schema for correctness and completeness.
"""

from crewai import LLM, Agent, Task, Crew, Process
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters
import os, sys, json, re
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import Config

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


def normalize_task_output(task_output):
    """Normalize raw task output into a clean string.

    Args:
        task_output: Output object which may be TaskOutput, dict, or str.

    Returns:
        str: A normalized string representation of the output.
    """
    if hasattr(task_output, "raw") and isinstance(task_output.raw, str):
        return task_output.raw.strip()
    if isinstance(task_output, dict):
        return json.dumps(task_output)
    if isinstance(task_output, str):
        return task_output.strip()
    return str(task_output)


def parse_validator_output(task_output_str):
    """Parse validator task output to extract JSON.

    Args:
        task_output_str (str): Raw output string containing JSON.

    Returns:
        dict: Parsed JSON object if found, otherwise {"raw_output": <original_str>}.
    """
    try:
        # Find first JSON block
        match = re.search(r"\{[\s\S]*\}", task_output_str)
        if match:
            return json.loads(match.group(0))
        return {"raw_output": task_output_str}
    except json.JSONDecodeError:
        return {"raw_output": task_output_str}


def collect_agent_outputs(tasks):
    """Collect outputs from Analyst and Validator tasks.

    Args:
        tasks (list[Task]): List of crew tasks executed.

    Returns:
        tuple:
            - analyst_result (str|None): Raw schema analysis JSON string.
            - validator_result (dict): Parsed validator JSON or raw output.
    """
    analyst_result, validator_result = None, {}

    for task in tasks:
        agent_role = getattr(task.agent, "role", "")
        task_output = getattr(task, "output", None)

        if not task_output:
            continue

        task_str = normalize_task_output(task_output)

        if "Analyst" in agent_role:
            analyst_result = task_str
        elif "Validator" in agent_role:
            validator_result = parse_validator_output(task_str)

    return analyst_result, validator_result


def execute_analysis_with_retry(crew, max_retries=3):
    """Run schema analysis with retry logic.

    Runs Analyst and Validator tasks, retries if validation fails,
    and feeds validator feedback back into the Analyst task.

    Args:
        crew (Crew): The Crew object with Analyst and Validator tasks.
        max_retries (int): Maximum number of attempts before giving up.

    Returns:
        tuple:
            - validator_result (dict): Final validator output.
            - analyst_result (str): Final analyst output.
            - validation_passed (bool): Whether schema validation succeeded.
            - attempt_count (int): Number of attempts executed.
    """
    print(f"\n🚀 Starting analysis with up to {max_retries} attempts...")

    base_analysis_description = crew.tasks[0].description
    final_analyst_result, final_validator_result, validation_passed = None, None, False

    for attempt in range(1, max_retries + 1):
        print(f"\n🔄 Attempt {attempt}/{max_retries}")

        try:
            crew.kickoff()

            # --- Collect outputs ---
            analyst_result, validator_result = collect_agent_outputs(crew.tasks)

            # Save latest results
            final_analyst_result, final_validator_result = (
                analyst_result,
                validator_result,
            )
            validation_passed = validator_result.get("validation_passed", False)

            if validation_passed:
                print("✅ Validation PASSED - Schema is accurate!")
                return (
                    final_validator_result,
                    final_analyst_result,
                    validation_passed,
                    attempt,
                )
            else:
                print(f"❌ Validation FAILED on attempt {attempt}")
                print(f"📋 Validator feedback: {validator_result}")

                # Feed back validator result for retry by updating Analyst task description.
                # This lets the Analyst improve its next attempt using validator feedback.
                crew.tasks[0].description = (
                    f"{base_analysis_description}\n\n"
                    f"⚠️ Previous validation results:\n{validator_result}\n"
                    f"Use this feedback to improve the next query."
                )

                if attempt < max_retries:
                    print("🔄 Retrying with validator feedback...")
                else:
                    print(
                        "⚠️ Maximum retries reached. Final results may contain inaccuracies."
                    )

        except Exception as e:
            print(f"❌ Error during attempt {attempt}: {str(e)}")
            if attempt == max_retries:
                raise
            print("🔄 Retrying due to execution error...")

    return final_validator_result, final_analyst_result, False, max_retries


def print_execution_summary(
    validator_result, analyst_result, validation_passed, attempt_count
):
    """Print a formatted summary of analysis execution.

    Args:
        validator_result (dict): Validator agent output.
        analyst_result (str): Analyst agent output.
        validation_passed (bool): Whether validation ultimately passed.
        attempt_count (int): Number of attempts made.
    """
    print("\n" + "=" * 60)
    print("📊 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"🔢 Total attempts: {attempt_count}")
    print(f"✅ Validation status: {'PASSED' if validation_passed else 'FAILED'}")

    print("\n" + "=" * 60)
    print("🧑‍💻 ANALYST RESULT")
    print("=" * 60)
    print(analyst_result)

    print("\n" + "=" * 60)
    print("📋 VALIDATOR RESULT")
    print("=" * 60)
    print(validator_result)
    print("=" * 60)


print("🔌 Connecting to MCP Server...")

with MCPServerAdapter(
    mcp_server_parameters, connect_timeout=MCP_CONNECTION_TIMEOUT
) as mcp_tools:
    print("✅ MCP Server connection established")

    available_tools = [tool.name for tool in mcp_tools]
    tool_list_str = ", ".join(available_tools)
    print(f"🛠️ Available MCP tools: {tool_list_str}")

    # 🧑‍💻 Analyst Agent: Extracts schema + table names
    database_analyst_agent = Agent(
        role="Expert Database Analyst",
        goal=f"""Extract all schema and table names from {DATABASE_NAME}.
        First attempt MUST use the provided MSSQL MCP tools, which are {tool_list_str}.
        If a tool does not return the required info, THEN and ONLY THEN run a custom T-SQL query via the MCP query execution tool.
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

    # 🔍 Validator Agent: Validates extracted schema for correctness
    validator_agent = Agent(
        role="Expert Database Validator",
        goal=f"""Validate the extracted schema and table names for {DATABASE_NAME}.
        First attempt MUST use the MSSQL MCP tools, which are {tool_list_str}.
        If information is incomplete, run a custom T-SQL query against sys.tables and sys.schemas using the MCP query execution tool.
        Return only JSON in the required format.""",
        backstory=f"""ROLE: SQL Server auditor.
        WORKFLOW:
        1. Always attempt to use MSSQL MCP tools first ({tool_list_str}).
        2. If they don’t return full details, fallback to sys.tables and sys.schemas queries through MCP.
        3. Never invent or assume missing details.
        4. Return results ONLY as structured JSON, no extra text. """,
        tools=mcp_tools,
        tools_only=True,
        verbose=True,
        llm=llm_cfg,
    )
    print("✅ Schema Validator Agent created")

    # Task 1: Extract all schema + table names
    database_analysis_task = Task(
        description=f"""Query {DATABASE_NAME} and return all schema and table names.

        RULES:
        1. First, use MCP tools: {tool_list_str}.
        2. If they don’t give the complete list, fallback to a direct T-SQL query against sys.tables and sys.schemas using MCP.
        3. Never assume or fabricate results.
        4. Output ONLY JSON in this format:
        [
            {{ "schema": "schema_name", "table": "table_name" }}
        ] """,
        expected_output="""Strict JSON array of objects with 'schema' and 'table' keys only.""",
        agent=database_analyst_agent,
    )
    print("✅ Schema Analysis Task configured")

    # Task 2: Validate the extracted schema list
    validation_task = Task(
        description=f"""Validate the schema and table list extracted from {DATABASE_NAME}.

        RULES:
        1. First, use MCP tools: {tool_list_str}.
        2. If they don’t give the complete list, fallback to direct T-SQL queries against sys.tables and sys.schemas using MCP.
        3. Never assume or fabricate issues.
        4. Output ONLY JSON in this format:
        {{
            "validation_passed": true|false,
            "issues": [
                {{
                "type": "missing_table|duplicate_table|missing_schema|other",
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

    # Crew = Analyst + Validator running sequentially
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
        validation_result, analyst_result, validation_success, total_attempts = execute_analysis_with_retry(crew=database_analysis_crew, max_retries=4)

        # Ensure output folder exists
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "tables.json")

        # --- Save Analyst output (tables) into tables.json ---
        if validation_success and analyst_result:
            with open(output_file, "w") as f:
                tables_json = json.loads(analyst_result)
                json.dump(tables_json, f, indent=2)
            print(f"💾 Saved extracted tables to {output_file}")
        else:
            print(f"⚠️ Validation failed hence no {output_file} saved.")

        print_execution_summary(
            validation_result, analyst_result, validation_success, total_attempts
        )

    except Exception as e:
        print(f"\n❌ Fatal error during analysis execution: {str(e)}")
        print("💡 Check that the MCP server is running and database is accessible")
        raise
