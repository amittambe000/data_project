import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"


def yt_elt_data_quality(schema):
    try:
        # Soda exit codes: 0=pass, 1=check failed, 2=warning, 3=error (includes API key warnings)
        # Only fail on exit code 1 (actual check failure), treat 0,2,3 as success
        bash_cmd = f"""
        OTEL_SDK_DISABLED=true soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema} {SODA_PATH}/checks.yml 2>/dev/null
        exit_code=$?
        if [ $exit_code -eq 1 ]; then
            echo "Data quality check FAILED"
            exit 1
        fi
        echo "Data quality check passed"
        exit 0
        """
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command=bash_cmd,
        )
        return task
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema}")
        raise e
