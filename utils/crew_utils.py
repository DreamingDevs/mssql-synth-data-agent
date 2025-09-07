from crewai import LLM, Agent, Task, Crew, Process
import os, sys, json, re

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