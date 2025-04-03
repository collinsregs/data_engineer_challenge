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
# Create categories table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT UNIQUE
        )
    """)

    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category_id INTEGER,
            FOREIGN KEY (category_id) REFERENCES categories (category_id)
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
            category_id INTEGER,
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (category_id) REFERENCES categories (category_id)
        )
    """)

    for file in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file)
        print('ingesting to silver file', file_path)

        if file.endswith(".json"):
            with open(file_path, 'r') as f:
                products_data = json.load(f)
                for product in products_data:
                    # Insert or get category_id
                    cursor.execute("INSERT OR IGNORE INTO categories (category_name) VALUES (?)", (product['category'],))
                    cursor.execute("SELECT category_id FROM categories WHERE category_name = ?", (product['category'],))
                    category_id = cursor.fetchone()[0]

                    # Insert or replace product
                    cursor.execute("INSERT OR REPLACE INTO products (product_id, product_name, category_id) VALUES (?, ?, ?)",
                                   (product['product_id'], product['product_name'], category_id))
                    conn.commit()

        elif file.endswith(".csv"):
            df_sales = pd.read_csv(file_path)
            for _, row in df_sales.iterrows():
                # Find category_id for each sale
                cursor.execute("SELECT category_id FROM products WHERE product_id = ?", (row['product_id'],))
                category_id = cursor.fetchone()[0]
                # Insert sale
                cursor.execute("INSERT INTO sales (product_id, sale_date, quantity, price, category_id) VALUES (?, ?, ?, ?, ?)",
                               (row['product_id'], row['sale_date'], row['quantity'], row['price'], category_id))
                conn.commit()
    conn.close()

__all__ = ['process_data']
    

