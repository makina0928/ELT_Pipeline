#========================================#
#   Load job ads from local CSV folder   #
#========================================#

import dlt
from dlt.sources.filesystem import readers
from dlt.extract import DltResource
import os
import logging
from pathlib import Path
import dotenv


# ==================== #
#   Logging Setup      #
# ==================== #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] | %(message)s",
)
log = logging.getLogger(__name__)


# ==================== #
#   Environment Setup  #
# ==================== #
dotenv.load_dotenv()
connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
if not connection_string:
    raise ValueError(" Missing POSTGRES_CONNECTION_STRING in .env")

pg_destination = dlt.destinations.postgres(connection_string)
base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "Data"

# ==================== #
#   DLT Resources      #
# ==================== #

@dlt.resource(name="loans", write_disposition="replace")
def loans() -> DltResource:
    """Stream loans data."""
    log.info(" Loading loans data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "fact_db"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  #  stream each batch manually
        yield batch

    log.info(" Loans data loaded successfully")

@dlt.resource(name="customers", write_disposition="replace")
def customers() -> DltResource:
    """Stream customers data."""
    log.info(" Loading customers data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "customer"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  #  manual yield again
        yield batch

    log.info(" Customers data loaded successfully")

@dlt.resource(name="locations", write_disposition="replace")
def locations() -> DltResource:
    """Stream location data."""
    log.info(" Loading locations data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "location"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  #  manual yield again
        yield batch

    log.info(" Locations data loaded successfully")

@dlt.source
def source():
    log.info(" Combining all DLT resources (loans, customers, locations)...")
    return [loans(), customers(), locations()]

# ==================== #
#   Pipeline Runner    #
# ==================== #
if __name__ == "__main__":
    log.info(" Starting ETL pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="etl_pipeline",
        destination=pg_destination,
        dataset_name="etl_data",
    )

    info = pipeline.run(source())

    # Corrected code to handle the return value properly
    log.info(f" Pipeline run completed with {len(info.loads_ids)} load package(s)")
    
    # Get the latest load package ID and check its status
    if info.loads_ids:
        load_id = info.loads_ids[-1]  # Get the most recent load ID
        log.info(f" Load ID: {load_id}")
        
        # Get table counts from the pipeline
        schema = pipeline.default_schema
        if schema is not None:
            for table_name in schema.tables.keys():
                if not table_name.startswith('_'):  # Skip internal dlt tables
                    log.info(f" Table '{table_name}' processed successfully")
    
    log.info(" ETL pipeline completed successfully!")