# generate_data.py
import os
import csv
import json
import random
import datetime

DATA_DIR = "../fake_sftp_data"  
num_products =20
num_days= 100

def generate_sales_data(date, num_records=100):
    filename = f"sales_data_{date.strftime('%Y-%m-%d')}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    product_ids = [f"P{i:03}" for i in range(1, num_products)]  # 20 product IDs
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['product_id', 'sale_date', 'quantity', 'price'])
        for _ in range(num_records):
            product_id = random.choice(product_ids)
            sale_date = date.strftime('%Y-%m-%d')
            quantity = random.randint(1, 10)
            price = round(random.uniform(10, 100), 2)
            writer.writerow([product_id, sale_date, quantity, price])
    print(f"Generated: {filename}")

def generate_product_info(date, num_products):
    filename = f"product_info_{date.strftime('%Y-%m-%d')}.json"
    filepath = os.path.join(DATA_DIR, filename)

    products = []
    for i in range(1, num_products + 1):
        product_id = f"P{i:03}"
        product_name = f"Product {i}"
        category = random.choice(['Electronics', 'Clothing', 'Books', 'Home'])
        products.append({
            'product_id': product_id,
            'product_name': product_name,
            'category': category
        })

    with open(filepath, 'w') as jsonfile:
        json.dump(products, jsonfile, indent=4)
    print(f"Generated: {filename}")

def generate_data_files(num_days):
    """Generates data files for multiple days."""
    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.date.today()
    for i in range(num_days):
        current_date = today - datetime.timedelta(days=i)
        generate_sales_data(current_date)
        generate_product_info(current_date, num_products)

if __name__ == "__main__":
    generate_data_files(num_days)