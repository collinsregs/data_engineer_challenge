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
from data_ingestion.ingest_to_bronze import ingest
from data_ingestion.process_data_to_silver import process_data

# Configuration (Assumptions: SFTP server details, data formats, etc.)

DB_PATH = "processed_data.db"
API_RATE_LIMIT = 100  # Requests per minute
LOCAL_DATA_DIR = "data"

app = Flask(__name__)
request_counts = {}

def run_pipeline():
    ingest()
    process_data(LOCAL_DATA_DIR,DB_PATH)

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