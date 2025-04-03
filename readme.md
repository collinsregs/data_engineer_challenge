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

* **SFTP Server:** A simple SFTP server is simulated for demonstration purposes. It is assumed that in a real-world scenario, a robust SFTP server would be available. Any username and password will work on this simulated server.
* **Data Source:** Sample CSV and JSON data files are generated for testing. In a real-world scenario, the data source would be a defined SFTP location with specific data formats.
* **Data Cleaning:** Basic data cleaning is performed to demonstrate the process. In a real-world scenario, more complex data quality measures might be necessary.
* **Security:** Basic security measures are implemented (rate limiting, basic authentication). In a production environment, more robust security measures would be required (e.g.more complex authentication(API keys, tokens) authorization).
* **Error Handling:** Basic error handling is implemented. More comprehensive error handling and logging would be necessary in a production environment.
* **Database:** SQLite is used for simplicity. In a production environment, a more scalable database system might be necessary.
* **Environment:** It is assumed that the user has python 3.7 or higher installed, and pip.
