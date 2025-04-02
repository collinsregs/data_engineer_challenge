import sqlite3
import os
import pandas as pd
import json
import datetime

def process_data(local_dir, db_path):
    """Processes downloaded data, stores sales and products in separate tables with a relationship."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT
        )
    """)

    # Create sales table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            sale_date TEXT,
            quantity INTEGER,
            price REAL,
            FOREIGN KEY (product_id) REFERENCES products (product_id)
        )
    """)

    for file in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file)

        if file.endswith(".json"):
            with open(file_path, 'r') as f:
                products_data = json.load(f)
                for product in products_data:
                    cursor.execute("INSERT OR REPLACE INTO products (product_id, product_name, category) VALUES (?, ?, ?)",
                                   (product['product_id'], product['product_name'], product['category']))

        elif file.endswith(".csv"):
            df_sales = pd.read_csv(file_path)
            for _, row in df_sales.iterrows():
                cursor.execute("INSERT INTO sales (product_id, sale_date, quantity, price) VALUES (?, ?, ?, ?)",
                               (row['product_id'], row['sale_date'], row['quantity'], row['price']))

    conn.commit()
    conn.close()

__all__ = ['process_data']
    

