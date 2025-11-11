#========================================#
#                                        #
#   Load data from local CSV folder      #
#                                        #
#========================================#

import dlt
import duckdb
import os
import logging
from pathlib import Path
import dotenv

# ======================================== #
#   Logging Setup                          #
# ======================================== #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] | %(message)s",
)
log = logging.getLogger(__name__)

# ======================================== #
#   Environment Setup (.env for Postgres)  #
# ======================================== #
dotenv.load_dotenv()
connection_string = os.getenv("POSTGRES_CONNECTION_STRING")

if not connection_string:
    raise ValueError("❌ Missing POSTGRES_CONNECTION_STRING in your .env file!")

pg_destination = dlt.destinations.postgres(connection_string)

# ======================================== #
#   Directory Setup                        #
# ======================================== #
base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "Data"
fact_folder = data_dir / "fact_db"
customer_folder = data_dir / "customer"
location_folder = data_dir / "location"

# Ensure staging truncation
dlt.config["load.truncate_staging_dataset"] = True


# ======================================== #
#   DLT Resources (Hybrid Chunking)        #
# ======================================== #
@dlt.resource(table_name="loans", write_disposition="replace")
def loans():
    query = f"""
        SELECT *
        FROM read_csv_auto('{os.path.join(fact_folder, "*.csv")}', 
                           union_by_name = true, 
                           filename = true)
    """
    con = duckdb.connect()
    con.execute(query)
    chunk_size = 10000
    while True:
        chunk = con.fetch_df_chunk(chunk_size)
        if chunk is None or chunk.empty:
            break
        for record in chunk.to_dict(orient="records"):
            yield record


@dlt.resource(table_name="customers", write_disposition="replace")
def customers():
    query = f"""
        SELECT *
        FROM read_csv_auto('{os.path.join(customer_folder, "*.csv")}',
                           union_by_name = true,
                           filename = true)
    """
    con = duckdb.connect()
    con.execute(query)
    chunk_size = 10000
    while True:
        chunk = con.fetch_df_chunk(chunk_size)
        if chunk is None or chunk.empty:
            break
        for record in chunk.to_dict(orient="records"):
            yield record


@dlt.resource(table_name="location", write_disposition="replace")
def locations():
    query = f"""
        SELECT *
        FROM read_csv_auto('{os.path.join(location_folder, "*.csv")}',
                           union_by_name = true,
                           filename = true)
    """
    con = duckdb.connect()
    con.execute(query)
    chunk_size = 10000
    while True:
        chunk = con.fetch_df_chunk(chunk_size)
        if chunk is None or chunk.empty:
            break
        for record in chunk.to_dict(orient="records"):
            yield record


@dlt.source
def source():
    return [loans(), customers(), locations()]