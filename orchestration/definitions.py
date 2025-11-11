# ==================== #
#       Imports        #
# ==================== #

from pathlib import Path
import sys
import os
import logging
from dotenv import load_dotenv
import dlt
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject


# ==================== #
#    Logging Setup     #
# ==================== #
BASE_DIR = Path(__file__).parents[1]
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

log_file = LOGS_DIR / "dagster_orchestration.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] | %(message)s",
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s | [%(levelname)s] | %(message)s"))
logging.getLogger().addHandler(console)

logging.info(" Starting Dagster orchestration setup...")


# ==================== #
#        Setup         #
# ==================== #

# Add ETL base directory to sys.path for imports
sys.path.insert(0, str(BASE_DIR))
from data_extraction.load_data import source  # Import the source function
logging.info(" Imported DLT source from data_extraction.load_data")

# Load environment variables explicitly
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
if not connection_string:
    logging.error(" Missing POSTGRES_CONNECTION_STRING in .env file")
    raise ValueError("Missing POSTGRES_CONNECTION_STRING in .env")

logging.info(f" Using PostgreSQL connection: {connection_string}")

# Initialize Dagster DLT resource
dlt_resource = DagsterDltResource()


# ==================== #
#       DLT ASSET      #
# ==================== #
@dlt_assets(
    dlt_source=source(),  # Call the source function to get the actual source
    dlt_pipeline=dlt.pipeline(
        pipeline_name="loans_pipeline",
        dataset_name="staging",
        destination=dlt.destinations.postgres(connection_string),  # Create destination inside pipeline
    ),
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Load local CSVs (customers, loans, location) into PostgreSQL via DLT pipeline."""
    logging.info(" [DLT] Starting pipeline execution through Dagster...")
    try:
        yield from dlt.run(context)  # Execute the DLT pipeline
        logging.info(" [DLT] Pipeline completed successfully via Dagster.")
    except Exception as e:
        logging.exception(f" [DLT] Pipeline execution failed: {e}")
        raise


# ==================== #
#       DBT ASSET      #
# ==================== #
dbt_project_dir = BASE_DIR / "postgres_dbt"
profiles_dir = dbt_project_dir  # profiles.yml is here

dbt_project = DbtProject(project_dir=dbt_project_dir, profiles_dir=profiles_dir)
dbt_resource = DbtCliResource(project_dir=dbt_project_dir, profiles_dir=profiles_dir)

dbt_project.prepare_if_dev()
logging.info(" [DBT] Project and profiles initialized successfully")

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Run and expose all dbt models (staging → transformed → marts)."""
    logging.info(" [DBT] Running dbt build...")
    try:
        yield from dbt.cli(["build"], context=context).stream()
        logging.info(" [DBT] dbt build completed successfully.")
    except Exception as e:
        logging.exception(f" [DBT] dbt build failed: {e}")
        raise


# ==================== #
#         JOBS         #
# ==================== #

# Define a job for DLT loading
job_dlt = dg.define_asset_job(
    "job_dlt",
    selection=dg.AssetSelection.keys(
        "dlt_source_loans",      # These will be the actual asset keys created by @dlt_assets
        "dlt_source_customers", 
        "dlt_source_locations"
    ),
)

# Define a job for dbt transformations
job_dbt = dg.define_asset_job(
    "job_dbt",
    selection=dg.AssetSelection.key_prefixes("staging", "transformed", "marts")
)


# ==================== #
#       SCHEDULES      #
# ==================== #
schedule_dlt = dg.ScheduleDefinition(
    job=job_dlt,
    cron_schedule="10 1 * * *"  # 01:10 UTC = 04:10 Nairobi
)
logging.info("Dagster schedule created for job_dlt (daily 04:10 Nairobi)")


# ==================== #
#       SENSORS        #
# ==================== #
@dg.multi_asset_sensor(
    monitored_assets=[
        dg.AssetKey("dlt_source_customers"),
        dg.AssetKey("dlt_source_loans"),
        dg.AssetKey("dlt_source_locations"),
    ],
    job=job_dbt,
)
def dlt_load_sensor(context):
    """Trigger dbt job after all DLT ingestion assets complete."""
    logging.info("⚡ Sensor triggered: DLT assets finished loading. Launching dbt job...")
    yield dg.RunRequest(run_key=None)


# ==================== #
#      DEFINITIONS     #
# ==================== #
defs = dg.Definitions(
    assets=[dlt_load, dbt_models],
    resources={"dlt": dlt_resource, "dbt": dbt_resource},
    jobs=[job_dlt, job_dbt],
    schedules=[schedule_dlt],
    sensors=[dlt_load_sensor]
)

logging.info(" Dagster definitions initialized successfully.")
logging.info(" Dagster orchestration setup complete — ready for `dagster dev`.")
