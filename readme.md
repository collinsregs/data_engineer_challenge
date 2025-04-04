# Data Pipeline and API Challenge

## Overview

This project implements a scalable data pipeline that ingests data from a simulated SFTP location, processes it for analytics, and provides access through an API with date-based filtering and cursor-based pagination.

## Project Structure

.
├── data_ingestion/                  # Data ingestion and processing scripts  
│   ├── ingest_to_bronze.py          # Script to download data from SFTP  
│   └── process_data_to_silver.py    # Script to process data and store in SQLite  
├── sftp_setup/                      # SFTP server setup and test data generation  
│   ├── fake_sftp_data               # holds test data for sftp access  
│   ├── generate_test_data.py        # Script to generate test data  
│   └── start_sftp.py                # Script to start a simple SFTP server  
├── data/                            # Directory for storing downloaded data               
├── database.db                      # SQLite database for storing processed data  
├── requirements.txt                 # Python package dependencies  
├── setup.py                         # Script to set up the environment and run all servers  
├── api.py                           # Flask API server  
└── pipeline.py                      # Script to run the data pipeline  


## Setup

1.  **Clone the Repository:**

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Run Setup Script:**

    Run the `setup.py` script to create a virtual environment, install required packages, generate test data, start the SFTP server, and start the Flask API server.

    ```bash
    python setup.py
    ```

    * This script will:
        * Create a virtual environment named `venv`.
        * Install the packages listed in `requirements.txt`.
        * Generate sample CSV and JSON data files in the `fake_sftp_data` directory.
        * Start a simple SFTP server (accessible with any username and password).
        * Start the Flask API server (accessible with the username `admin` and password `password123` using basic authentication).

3.  **Run the pipeline:**
    Run the `pipeline.py` script to ingest the fake data from the sftp server and populate the database.

    ```bash
    python pipeline.py
    ```

## API Usage

The API provides access to the processed data stored in the SQLite database.

### Base URL

`http://localhost:5000`

### Available Routes

* **`/products` (GET):**
    * Returns a list of products.
    * Parameters:
        * `limit` (optional, default: 10): Number of products to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/sales` (GET):**
    * Returns a list of sales.
    * Parameters:
        * `limit` (optional, default: 10): Number of sales to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/categories` (GET):**
    * Returns a list of unique categories.
    * Parameters:
        * `limit` (optional, default: 10): Number of categories to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/sales/daily_count` (GET):**
    * Returns the count of sales per day.
    * Parameters:
        * `limit` (optional, default: 10): Number of results to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/sales/product_daily_count` (GET):**
    * Returns the count of sales per product per day.
    * Parameters:
        * `limit` (optional, default: 10): Number of results to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/sales/category_sales` (GET):**
    * Returns the total sales amount and count per category.
    * Parameters:
        * `limit` (optional, default: 10): Number of results to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/sales/filtered` (GET):**
    * Returns filtered sales based on date and/or product.
    * Parameters:
        * `date` (optional): Date filter (e.g., `2025-04-02`).
        * `product_id` (optional): Product ID filter.
        * `category` (optional): Category filter.
        * `limit` (optional, default: 10): Number of sales to return per page.
        * `cursor` (optional): Cursor for pagination.
* **`/help` (GET):**
    * Returns a list of available routes and their usage.

### Pagination

Cursor-based pagination is implemented to handle large datasets. The `next_cursor` field in the API response provides the cursor for the next page.

### Rate Limiting

The API implements basic rate limiting to prevent abuse. A maximum of 100 requests per minute is allowed per IP address.

### Example `curl` Command

```bash
curl -u admin:password123 http://localhost:5000/products
```
## Assumptions

* **SFTP Server:** It is assumed that a robust SFTP server would be available in a real-world scenario. A simple SFTP server is simulated for demonstration purposes. Any username and password will work on this simulated server.
* **Data Source:** It is assumed that the data source would be a defined SFTP location with specific data formats in a real-world scenario. Sample CSV and JSON data files are generated for testing. It is further assumed that each file type consistently corresponds to a single data type.
* **Data Cleaning:** It is assumed that more complex data quality measures might be necessary in a real-world scenario. Basic data cleaning is performed to demonstrate the process.
* **Security:** It is assumed that more robust security measures (e.g., more complex authentication (API keys, tokens), authorization) would be required in a production environment. Basic security measures are implemented (rate limiting, basic authentication).
* **Error Handling:** It is assumed that more comprehensive error handling and logging would be necessary in a production environment. Basic error handling is implemented.
* **Database:** It is assumed that a more scalable database system might be necessary in a production environment. SQLite is used for simplicity.
* **Environment:** It is assumed that the user has Python 3.7 or higher installed, and pip.