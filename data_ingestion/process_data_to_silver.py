import sqlite3
import os
import pandas as pd
import json
import datetime
import logging
import shutil


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_data(data, data_type):
    """Performs basic data cleaning based on data type."""
    if data_type == 'product':
        if not isinstance(data.get('product_id'), str):
            data['product_id'] = str(data.get('product_id'))
        if not isinstance(data.get('product_name'), str):
            data['product_name'] = str(data.get('product_name'))
        if not isinstance(data.get('category'), str):
            data['category'] = str(data.get('category'))
        return data
    elif data_type == 'sale':
        if not isinstance(data.get('product_id'), str):
            data['product_id'] = str(data.get('product_id'))
        try:
            datetime.datetime.strptime(data.get('sale_date'), '%Y-%m-%d') 
        except (ValueError, TypeError):
            data['sale_date'] = None 
        if not isinstance(data.get('quantity'), int):
            try:
                data['quantity'] = int(data.get('quantity'))
            except (ValueError, TypeError):
                data['quantity'] = 0 
        if not isinstance(data.get('price'), (int, float)):
            try:
                data['price'] = float(data.get('price'))
            except (ValueError, TypeError):
                data['price'] = 0.0 
        return data
    return data

def process_data(local_dir, db_path):
    """Processes downloaded data, stores sales and products in separate tables with batch processing for efficiency."""
    
    # Set up the database with optimized settings
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # Use Write-Ahead Logging for better concurrency
    conn.execute("PRAGMA synchronous=NORMAL")  # Reduce fsync calls for better performance
    conn.execute("PRAGMA cache_size=10000")  # Increase cache size
    conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
    
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT UNIQUE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category_id INTEGER,
            FOREIGN KEY (category_id) REFERENCES categories (category_id)
        )
    """)

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
    
    # Create category_name to ID mapping cache
    cursor.execute("SELECT category_id, category_name FROM categories")
    category_cache = {name: id for id, name in cursor.fetchall()}
    
    # Set up directory for unknown files
    unknown_files_dir = os.path.join(local_dir, "unknown_files")
    os.makedirs(unknown_files_dir, exist_ok=True)
    
    # Process files in batches
    BATCH_SIZE = 1000  # Adjust based on your data size
    
    file_count = 0
    total_files = len([f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))])
    
    for file in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file)
        
        if not os.path.isfile(file_path):
            continue
            
        file_count += 1
        logging.info(f"Processing file {file_count}/{total_files}: {file}")
        
        if file.endswith(".json"):
            # Process JSON product files
            with open(file_path, 'r') as f:
                products_data = json.load(f)
                
            # Prepare batches
            product_batch = []
            new_categories = set()
            
            for product in products_data:
                cleaned_product = clean_data(product, 'product')
                category = cleaned_product['category']
                
                # Track new categories
                if category not in category_cache:
                    new_categories.add(category)
                
                product_batch.append(cleaned_product)
                
                # Process batch if it reaches the batch size
                if len(product_batch) >= BATCH_SIZE:
                    _batch_process_products(conn, cursor, product_batch, category_cache, new_categories)
                    product_batch = []
                    new_categories = set()
            
            # Process remaining items
            if product_batch:
                _batch_process_products(conn, cursor, product_batch, category_cache, new_categories)
                
        elif file.endswith(".csv"):
            # Process CSV sales files - use pandas efficiently
            df_sales = pd.read_csv(file_path)
            
            # Clean data using vectorized operations where possible
            if 'product_id' in df_sales.columns:
                df_sales['product_id'] = df_sales['product_id'].astype(str)
            
            # Validate dates
            if 'sale_date' in df_sales.columns:
                df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Convert numeric columns
            if 'quantity' in df_sales.columns:
                df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(0).astype(int)
            
            if 'price' in df_sales.columns:
                df_sales['price'] = pd.to_numeric(df_sales['price'], errors='coerce').fillna(0).astype(float)
            
            # Get category_ids for all product_ids at once
            unique_product_ids = df_sales['product_id'].unique()
            product_to_category = {}
            
            # Break into smaller chunks if there are many unique products
            for i in range(0, len(unique_product_ids), 500):
                chunk = unique_product_ids[i:i+500]
                placeholders = ','.join(['?'] * len(chunk))
                cursor.execute(f"SELECT product_id, category_id FROM products WHERE product_id IN ({placeholders})", chunk)
                product_to_category.update(dict(cursor.fetchall()))
            
            # Process in batches
            total_rows = len(df_sales)
            for i in range(0, total_rows, BATCH_SIZE):
                if i % (BATCH_SIZE * 10) == 0:
                    logging.info(f"Processing sales batch {i}-{min(i+BATCH_SIZE, total_rows)} of {total_rows}")
                
                batch_df = df_sales.iloc[i:i+BATCH_SIZE]
                sale_records = []
                
                for _, row in batch_df.iterrows():
                    row_dict = row.to_dict()
                    product_id = row_dict.get('product_id')
                    
                    if product_id in product_to_category:
                        category_id = product_to_category[product_id]
                        sale_records.append((
                            product_id,
                            row_dict.get('sale_date'),
                            row_dict.get('quantity', 0),
                            row_dict.get('price', 0.0),
                            category_id
                        ))
                
                if sale_records:
                    cursor.executemany(
                        "INSERT INTO sales (product_id, sale_date, quantity, price, category_id) VALUES (?, ?, ?, ?, ?)",
                        sale_records
                    )
                    conn.commit()
                
        else:
            logging.warning(f"Unknown file type: {file}. Moving to unknown_files directory.")
            shutil.move(file_path, os.path.join(unknown_files_dir, file))
    
    # Final optimization: create indexes for faster queries
    logging.info("Creating indexes for better query performance...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales (product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_category_id ON sales (category_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales (sale_date)")
    
    conn.commit()
    conn.close()
    logging.info("Data processing completed successfully.")

def _batch_process_products(conn, cursor, product_batch, category_cache, new_categories):
    """Helper function to process product batches and update category cache."""
    # First insert any new categories
    if new_categories:
        category_values = [(category,) for category in new_categories]
        cursor.executemany("INSERT OR IGNORE INTO categories (category_name) VALUES (?)", category_values)
        conn.commit()
        
        # Update category cache with new IDs
        for category in new_categories:
            if category not in category_cache:
                cursor.execute("SELECT category_id FROM categories WHERE category_name = ?", (category,))
                category_cache[category] = cursor.fetchone()[0]
    
    # Now batch insert products
    product_values = [
        (p['product_id'], p['product_name'], category_cache[p['category']])
        for p in product_batch
    ]
    
    cursor.executemany(
        "INSERT OR REPLACE INTO products (product_id, product_name, category_id) VALUES (?, ?, ?)",
        product_values
    )
    conn.commit()

__all__ = ['process_data']