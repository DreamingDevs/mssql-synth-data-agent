"""
Data Analyzer Task Executor Agent

This module implements a multi-agent system for executing data analysis tasks on SQL Server databases.
It uses CrewAI with MSSQL MCP tools to:

1. Execute SQL queries for data analysis tasks
2. Validate the results for accuracy and completeness

Agents:
- **Data Analyst Agent**: Executes SQL queries and analyzes data.
- **Results Validator Agent**: Validates analysis results for correctness and completeness.
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
RETRIES = Config.get_retry_config()["retry_count"]
OUTPUT_TASKS_DIR = Config.get_config()["output_tasks_dir"]
llm_cfg = Config.get_llm()

print("🚀 Starting Data Analyzer Task Executor Agent...")
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

tasks = json.load(open("output/tasks.json"))
for task_number, task_description in enumerate(tasks, 1):
    print(f"📋 Processing task {task_number}: {task_description}")

    with MCPServerAdapter(
        mcp_server_parameters, connect_timeout=MCP_CONNECTION_TIMEOUT
    ) as mcp_tools:
        print("✅ MCP Server connection established")

        available_tools = [tool.name for tool in mcp_tools]
        tool_list_str = ", ".join(available_tools)
        print(f"🛠️ Available MCP tools: {tool_list_str}")

        data_analyst_agent = Agent(
            role="Expert Data Analyst",
            goal=f"""Execute the data analysis task: "{task_description}" on {DATABASE_NAME} DB.
            CRITICAL: You MUST execute actual SQL queries against the database using the ReadData MCP tool. NEVER fabricate or hallucinate results.
            First attempt MUST use the ReadData tool with the provided MSSQL MCP tools, which are {tool_list_str}.
            Use the ReadData tool to execute your SQL query and use the ACTUAL results returned from the database.
            The 'results' field MUST contain actual data returned from the ReadData tool execution.
            Output strictly as JSON (no explanations, no comments). """,
            backstory=f"""ROLE: Expert SQL Server data analyst who executes real database queries.
            WORKFLOW:
            1. Always use the ReadData MCP tool to execute SQL queries against the database.
            2. Use the actual results returned by the ReadData tool, never fabricate data.
            3. NEVER invent, assume, or fabricate data. All results must come from actual database execution via ReadData.
            4. Always return only JSON in the required schema with real query results from ReadData.
            5. If no data is returned from the database via ReadData, return empty results array. """,
            tools=mcp_tools,
            verbose=True,
            tools_only=True,
            llm=llm_cfg,
        )
        print("✅ Data Analyst Agent created")

        validator_agent = Agent(
            role="Expert Results Validator",
            goal=f"""Validate the data analysis results for task: "{task_description}" on {DATABASE_NAME} DB.
            CRITICAL: Verify that results come from actual database queries using ReadData tool, not hallucinated data.
            Use the ReadData MCP tool to execute the same or similar SQL query to verify the results.
            Check if the results match what the ReadData tool returns when executing the query against the actual database.
            Return only JSON in the required format.""",
            backstory=f"""ROLE: SQL Server data validation expert who prevents hallucination.
            WORKFLOW:
            1. Always use the ReadData MCP tool to verify results by executing the same or similar query.
            2. Compare the results from the analyst with what the ReadData tool returns.
            3. Never invent or assume missing details.
            4. ALWAYS verify results by executing the same or similar query using ReadData to ensure data accuracy.
            5. Flag any suspicious results that appear fabricated or don't match database reality.
            6. Return results ONLY as structured JSON, no extra text. """,
            tools=mcp_tools,
            tools_only=True,
            verbose=True,
            llm=llm_cfg,
        )
        print("✅ Results Validator Agent created")

        data_analysis_task = Task(
            description=f"""Execute the following data analysis task on {DATABASE_NAME} DB: "{task_description}"

            CRITICAL RULES:
            1. Use the ReadData MCP tool to execute your SQL query against the database.
            2. NEVER assume, fabricate, or hallucinate results. All data must come from actual database execution via ReadData.
            3. Execute the SQL query using the ReadData tool and use the ACTUAL results returned from the database.
            4. If the ReadData tool returns no results, use an empty array for 'results' and 0 for 'row_count'.
            5. The 'results' field must contain the exact data returned by the ReadData tool.
            6. Output ONLY JSON in this format:
            {{
                "task": "{task_description}",
                "query": "SQL query used",
                "results": [
                    {{"column1": "actual_value1", "column2": "actual_value2", ...}},
                    {{"column1": "actual_value3", "column2": "actual_value4", ...}}
                ],
                "row_count": actual_number_of_rows_returned
            }} """,
            expected_output="""Strict JSON object with 'task', 'query', 'results', 'row_count' keys containing actual database results.""",
            agent=data_analyst_agent,
        )
        print("✅ Data Analysis Task configured")

        validation_task = Task(
            description=f"""Validate the data analysis results for task: "{task_description}" on {DATABASE_NAME} DB.

            CRITICAL RULES:
            1. Use the ReadData MCP tool to execute the same or similar SQL query to verify the results.
            2. Never assume or fabricate issues.
            3. ALWAYS execute the same or similar SQL query using ReadData to verify the results are accurate and not hallucinated.
            4. Check if the results match what the ReadData tool returns when executing the query against the actual database.
            5. Flag validation_passed as false if results appear fabricated or don't match database reality.
            6. Use the actual results returned by ReadData for verification.
            7. Output ONLY JSON in this format:
            {{
                "validation_passed": true|false,
                "issues": [
                    {{
                    "type": "incorrect_data|missing_data|invalid_query|hallucinated_results|other",
                    "description": "Explanation of the issue",
                    "details": "Detailed explanation"
                    }}
                ],
                "message": "<summary string>",
                "verification_query": "SQL query used for verification"
            }}
            """,
            expected_output="Strict JSON object with validation_passed, issues[], message, and verification_query keys.",
            agent=validator_agent,
            context=[data_analysis_task],
        )

        print("✅ Results Validation Task configured")

        # Crew = Analyst + Validator running sequentially
        data_analysis_crew = Crew(
            agents=[data_analyst_agent, validator_agent],
            tasks=[data_analysis_task, validation_task],
            process=Process.sequential,
        )
        print("✅ Data Analysis Crew assembled")
        print(f"👥 Crew members: {len(data_analysis_crew.agents)} agents")
        print(f"📝 Total tasks: {len(data_analysis_crew.tasks)} tasks")

        try:
            validation_result, analyst_result, validation_success, total_attempts = execute_analysis_with_retry(crew=data_analysis_crew, max_retries=RETRIES)

            os.makedirs(OUTPUT_TASKS_DIR, exist_ok=True)
            output_file = os.path.join(OUTPUT_TASKS_DIR, f"task_{task_number}.json")

            if validation_success and analyst_result:
                with open(output_file, "w") as f:
                    task_result_json = json.loads(analyst_result)
                    json.dump(task_result_json, f, indent=2)
                print(f"💾 Saved task {task_number} results to {output_file}")
            else:
                print(f"⚠️ Validation failed hence no {output_file} saved.")

            print_execution_summary(
                validation_result, analyst_result, validation_success, total_attempts
            )

        except Exception as e:
            print(f"\n❌ Fatal error during analysis execution: {str(e)}")
            print("💡 Check that the MCP server is running and database is accessible")
            raise
