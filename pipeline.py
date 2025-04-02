# data_pipeline.py
import os
import io
import json
import csv
import datetime
import pandas as pd
import paramiko
import sqlite3
import hashlib
import time
from flask import Flask, request, jsonify

# Configuration (Assumptions: SFTP server details, data formats, etc.)
SFTP_HOST = "localhost"  # Example public SFTP server
SFTP_PORT = 22
SFTP_USER = "demo"
SFTP_PASS = "password"
SFTP_REMOTE_DIR = "/fake_sftp"
LOCAL_DATA_DIR = "data"
DB_PATH = "processed_data.db"
API_RATE_LIMIT = 100  # Requests per minute

app = Flask(__name__)
request_counts = {}

def connect_sftp():
    """Connects to the SFTP server."""
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp, transport
    except Exception as e:
        print(f"Error connecting to SFTP: {e}")
        return None, None

def download_sftp_files(sftp, remote_dir, local_dir):
    """Downloads files from SFTP."""
    try:
        files = sftp.listdir(remote_dir)
        os.makedirs(local_dir, exist_ok=True)
        for file in files:
            remote_path = f"{remote_dir}/{file}"
            local_path = os.path.join(local_dir, file)
            sftp.get(remote_path, local_path)
            print(f"Downloaded: {file}")
    except Exception as e:
        print(f"Error downloading files: {e}")

def process_data(local_dir, db_path):
    """Processes downloaded data and stores it in a SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            data TEXT
        )
    """)
    for file in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file)
        if file.endswith(".csv"):
            df = pd.read_csv(file_path)
            # Example flattening and cleaning (replace with your actual logic)
            df['data'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
            for _, row in df.iterrows():
                cursor.execute("INSERT INTO processed_data (date, data) VALUES (?, ?)",
                               (datetime.datetime.now().isoformat(), row['data']))
        elif file.endswith(".json"):
            with open(file_path, 'r') as f:
                data = json.load(f)
            # Example flattening and cleaning (replace with your actual logic)
            flat_data = json.dumps(data)
            cursor.execute("INSERT INTO processed_data (date, data) VALUES (?, ?)",
                           (datetime.datetime.now().isoformat(), flat_data))
    conn.commit()
    conn.close()

def run_pipeline():
    """Runs the data pipeline."""
    sftp, transport = connect_sftp()
    if sftp:
        download_sftp_files(sftp, SFTP_REMOTE_DIR, LOCAL_DATA_DIR)
        sftp.close()
        transport.close()
        process_data(LOCAL_DATA_DIR, DB_PATH)
        print("Pipeline completed.")

def generate_cursor(last_id, limit):
    """Generates a cursor for pagination."""
    return hashlib.md5(f"{last_id}-{limit}".encode()).hexdigest()

def validate_cursor(cursor, last_id, limit):
    """Validates a cursor."""
    return cursor == generate_cursor(last_id, limit)

def rate_limit():
  ip_address = request.remote_addr
  now = time.time()
  if ip_address not in request_counts:
    request_counts[ip_address] = []
  requests = request_counts[ip_address]
  requests = [r for r in requests if r > now - 60]
  if len(requests) >= API_RATE_LIMIT:
    return False
  requests.append(now)
  request_counts[ip_address] = requests
  return True

@app.route('/data', methods=['GET'])
def get_data():
    if not rate_limit():
      return jsonify({"error": "Rate limit exceeded"}), 429

    date_filter = request.args.get('date')
    limit = int(request.args.get('limit', 10))
    cursor = request.args.get('cursor')
    last_id = 0

    conn = sqlite3.connect(DB_PATH)
    cursor_db = conn.cursor()

    query = "SELECT id, date, data FROM processed_data"
    params = []

    if date_filter:
        query += " WHERE date LIKE ?"
        params.append(f"{date_filter}%")

    if cursor:
        cursor_db.execute("SELECT id from processed_data ORDER BY id DESC LIMIT 1")
        result = cursor_db.fetchone()
        if result:
          last_id = result[0]
        if not validate_cursor(cursor, last_id, limit):
            return jsonify({"error": "Invalid cursor"}), 400
        query += " AND id > ?"
        params.append(last_id)

    query += " LIMIT ?"
    params.append(limit)

    cursor_db.execute(query, params)
    rows = cursor_db.fetchall()

    data = []
    for row in rows:
        data.append({"id": row[0], "date": row[1], "data": json.loads(row[2])})

    next_cursor = None
    if len(rows) == limit:
        next_cursor = generate_cursor(rows[-1][0], limit)

    conn.close()
    return jsonify({"data": data, "next_cursor": next_cursor})

if __name__ == "__main__":
    run_pipeline()
    app.run(debug=True, host='0.0.0.0', port=5000)