
from data_ingestion.ingest_to_bronze import ingest
from data_ingestion.process_data_to_silver import process_data

DB_PATH = "database.db"
LOCAL_DATA_DIR = "data"



def run_pipeline():
    ingest(LOCAL_DATA_DIR)
    process_data(LOCAL_DATA_DIR,DB_PATH)

if __name__ == "__main__":
    run_pipeline()
    