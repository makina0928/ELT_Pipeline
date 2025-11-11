# #========================================#
# #                                        #
# #   Load job ads from local CSV folder   #
# #                                        #
# #========================================#

# import dlt
# import duckdb
# import os
# from pathlib import Path

# import dotenv
# dotenv.load_dotenv()
# connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
# pg_destination = dlt.destinations.postgres(connection_string)

# # Resolve paths relative to this script
# base_dir = Path(__file__).resolve().parent
# data_dir = base_dir / "Data"
# fact_folder = data_dir / "fact_db"
# customer_folder = data_dir / "customer"
# location_folder = data_dir / "location"

# # Ensure staging truncation
# dlt.config["load.truncate_staging_dataset"] = True

# @dlt.resource(table_name="loans", write_disposition="replace")
# def loans():
#     query = f"""
#         SELECT *
#         FROM read_csv_auto('{os.path.join(fact_folder, "*.csv")}', 
#                            union_by_name = true, 
#                            filename = true)
#     """
#     df = duckdb.sql(query).df()
#     for record in df.to_dict(orient="records"):
#         yield record


# @dlt.resource(table_name="customers", write_disposition="replace")
# def customers():
#     query = f"""
#         SELECT *
#         FROM read_csv_auto('{os.path.join(customer_folder, "*.csv")}',
#                            union_by_name = true,
#                            filename = true)
#     """
#     df = duckdb.sql(query).df()
#     for record in df.to_dict(orient="records"):
#         yield record


# @dlt.resource(table_name="location", write_disposition="replace")
# def locations():
#     query = f"""
#         SELECT *
#         FROM read_csv_auto('{os.path.join(location_folder, "*.csv")}',
#                            union_by_name = true,
#                            filename = true)
#     """
#     df = duckdb.sql(query).df()
#     for record in df.to_dict(orient="records"):
#         yield record


# @dlt.source
# def source():
#     return [loans(), customers(), locations()]


# if __name__ == "__main__":
#     dlt.pipeline(
#         pipeline_name="loans_pipeline",
#         dataset_name="staging",
#         destination=pg_destination,
#     ).run(source())



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
    raise ValueError("❌ Missing POSTGRES_CONNECTION_STRING in .env")

pg_destination = dlt.destinations.postgres(connection_string)
base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "Data"


# ==================== #
#   DLT Resources      #
# ==================== #

@dlt.resource(name="loans", write_disposition="replace")
def loans() -> DltResource:
    """Stream loans data."""
    log.info("📊 Loading loans data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "fact_db"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  # ✅ stream each batch manually
        yield batch

    log.info("✅ Loans data loaded successfully")


@dlt.resource(name="customers", write_disposition="replace")
def customers() -> DltResource:
    """Stream customers data."""
    log.info("📊 Loading customers data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "customer"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  # ✅ manual yield again
        yield batch

    log.info("✅ Customers data loaded successfully")


@dlt.resource(name="locations", write_disposition="replace")
def locations() -> DltResource:
    """Stream location data."""
    log.info("📊 Loading locations data via readers().read_csv_duckdb(chunk_size=1000)...")
    reader = readers(
        bucket_url=str(data_dir / "location"),
        file_glob="*.csv"
    ).read_csv_duckdb(chunk_size=1000, header=True)
    
    for batch in reader:  # ✅ manual yield again
        yield batch

    log.info("✅ Locations data loaded successfully")


@dlt.source
def source():
    log.info("🔗 Combining all DLT resources (loans, customers, locations)...")
    return [loans(), customers(), locations()]


# ==================== #
#   Pipeline Runner    #
# ==================== #
if __name__ == "__main__":
    log.info("🚀 Starting ETL pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name="etl_pipeline",
        destination=pg_destination,
        dataset_name="etl_data",
    )

    info = pipeline.run(source())

    # Corrected code to handle the return value properly
    log.info(f"✅ Pipeline run completed with {len(info.loads_ids)} load package(s)")
    
    # Get the latest load package ID and check its status
    if info.loads_ids:
        load_id = info.loads_ids[-1]  # Get the most recent load ID
        log.info(f"✅ Load ID: {load_id}")
        
        # Get table counts from the pipeline
        schema = pipeline.default_schema
        if schema is not None:
            for table_name in schema.tables.keys():
                if not table_name.startswith('_'):  # Skip internal dlt tables
                    log.info(f"✅ Table '{table_name}' processed successfully")
    
    log.info("🎉 ETL pipeline completed successfully!")






#========================================#
#                                        #
#   Load job ads from local CSV folder   #
#                                        #
#========================================#

# import dlt
# import duckdb
# import os
# import logging
# from pathlib import Path
# import dotenv


# # ==================== #
# #     Logging Setup    #
# # ==================== #
# log_file = Path(__file__).resolve().parent / "dlt_load.log"
# logging.basicConfig(
#     filename=log_file,
#     level=logging.INFO,
#     format="%(asctime)s | [%(levelname)s] | %(message)s",
# )

# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# console.setFormatter(logging.Formatter("%(asctime)s | [%(levelname)s] | %(message)s"))
# logging.getLogger().addHandler(console)

# logging.info("🚀 Starting DLT load_data.py initialization...")


# # ==================== #
# #   Environment Setup  #
# # ==================== #
# dotenv.load_dotenv()
# connection_string = os.getenv("POSTGRES_CONNECTION_STRING")

# if not connection_string:
#     logging.error("❌ POSTGRES_CONNECTION_STRING missing in .env file.")
#     raise ValueError("Missing POSTGRES_CONNECTION_STRING in .env")

# logging.info(f"✅ Using connection string: {connection_string}")

# # Initialize DLT PostgreSQL destination
# pg_destination = dlt.destinations.postgres(connection_string)


# # ==================== #
# #   Directory Setup    #
# # ==================== #
# base_dir = Path(__file__).resolve().parent
# data_dir = base_dir / "Data"
# fact_folder = data_dir / "fact_db"
# customer_folder = data_dir / "customer"
# location_folder = data_dir / "location"

# logging.info(f"📂 Data directory: {data_dir}")
# logging.info(f"📂 fact_db folder: {fact_folder}")
# logging.info(f"📂 customer folder: {customer_folder}")
# logging.info(f"📂 location folder: {location_folder}")

# # Ensure staging truncation
# dlt.config["load.truncate_staging_dataset"] = True
# logging.info("🧹 DLT staging truncation enabled.")


# # ==================== #
# #     DLT Resources    #
# # ==================== #
# @dlt.resource(table_name="loans", write_disposition="replace")
# def loans():
#     try:
#         query = f"""
#             SELECT *
#             FROM read_csv_auto('{os.path.join(fact_folder, "*.csv")}', 
#                                union_by_name = true, 
#                                filename = true)
#         """
#         logging.info("📊 Loading loans data from CSVs...")
#         df = duckdb.sql(query).df()
#         logging.info(f"✅ Loans data loaded: {len(df)} records")
#         for record in df.to_dict(orient="records"):
#             yield record
#     except Exception as e:
#         logging.exception(f"❌ Error while reading loans data: {e}")
#         raise


# @dlt.resource(table_name="customers", write_disposition="replace")
# def customers():
#     try:
#         query = f"""
#             SELECT *
#             FROM read_csv_auto('{os.path.join(customer_folder, "*.csv")}',
#                                union_by_name = true,
#                                filename = true)
#         """
#         logging.info("📊 Loading customers data from CSVs...")
#         df = duckdb.sql(query).df()
#         logging.info(f"✅ Customers data loaded: {len(df)} records")
#         for record in df.to_dict(orient="records"):
#             yield record
#     except Exception as e:
#         logging.exception(f"❌ Error while reading customers data: {e}")
#         raise


# @dlt.resource(table_name="location", write_disposition="replace")
# def locations():
#     try:
#         query = f"""
#             SELECT *
#             FROM read_csv_auto('{os.path.join(location_folder, "*.csv")}',
#                                union_by_name = true,
#                                filename = true)
#         """
#         logging.info("📊 Loading location data from CSVs...")
#         df = duckdb.sql(query).df()
#         logging.info(f"✅ Location data loaded: {len(df)} records")
#         for record in df.to_dict(orient="records"):
#             yield record
#     except Exception as e:
#         logging.exception(f"❌ Error while reading location data: {e}")
#         raise


# @dlt.source
# def source():
#     logging.info("🔗 Combining all DLT resources into a single source...")
#     return [loans(), customers(), locations()]


# # ==================== #
# #     Direct Run       #
# # ==================== #
# if __name__ == "__main__":
#     try:
#         logging.info("🚀 Starting DLT pipeline execution (manual mode)...")
#         pipeline = dlt.pipeline(
#             pipeline_name="loans_pipeline",
#             dataset_name="staging",
#             destination=pg_destination,
#         )
#         info = pipeline.run(source())
#         logging.info(f"✅ Pipeline completed successfully: {info}")
#     except Exception as e:
#         logging.exception(f"❌ Pipeline execution failed: {e}")
#         raise
