import logging
import re
import subprocess
import sys


def run_dbt_pipeline() -> None:
    """Run the dbt pipeline and associated tests."""
    command = ("uv", "run", "dbt", "run")

    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Unexpected error when running dbt")
        raise RuntimeError("Failed to invoke dbt run") from exc

    if result.returncode != 0:
        logging.error("DBT pipeline failed with return code %s", result.returncode)
        if result.stdout:
            logging.error("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            logging.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError("dbt run exited with a non-zero status")

    logging.info("DBT pipeline completed successfully")

    test_command = ("uv", "run", "dbt", "test")

    try:
        test_result = subprocess.run(
            test_command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Unexpected error when running dbt tests")
        raise RuntimeError("Failed to invoke dbt test") from exc

    if test_result.returncode == 0:
        logging.info("DBT tests completed successfully")
        return

    if test_result.stdout:
        pattern = re.compile(r"(Failure in test.*?(?:\n\s*\n|$))", re.DOTALL)
        error_blocks = pattern.findall(test_result.stdout)
        if error_blocks:
            for block in error_blocks:
                for line in block.strip().splitlines():
                    logging.error(line)
        else:
            logging.error("No failure blocks found in dbt test output")

    if test_result.stderr:
        logging.error("dbt test stderr:\n%s", test_result.stderr)

    sys.exit(1)
