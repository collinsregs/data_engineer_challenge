# api_config.py
from flask import Flask, request, jsonify, Response
import time
import hashlib
import json
import sqlite3
from functools import wraps

app = Flask(__name__)

DB_PATH = "database.db"
API_RATE_LIMIT = 100  # Requests per minute

request_counts = {}
def check_auth(username, password):
    """This function checks if the username / password combinations are valid."""
    return username == 'admin' and password == 'password123' 

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

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

def execute_query(query, params=()):
    """Executes a database query and returns the result."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows

def paginate_query(query, params, limit, cursor_val):
    """Paginates a query based on limit and cursor."""
    if cursor_val:
        cursor_id = cursor_val.split('-')[0]  # Extract the ID from the cursor
        query += " AND rowid > ?"
        params.append(cursor_id)

    query += " LIMIT ?"
    params.append(limit)

    return query, params

def fetch_paginated_data(query, params, limit, cursor_val):
    """Fetches paginated data and returns the result with a cursor."""
    query, params = paginate_query(query, params, limit, cursor_val)
    rows = execute_query(query, params)
    next_cursor = None

    if len(rows) == limit:
        last_row_id = rows[-1][0]  # Assuming first column is the row ID
        next_cursor = f"{last_row_id}-{limit}"

    return rows, next_cursor

@app.before_request
def before_request():
    if not rate_limit():
        return jsonify({"error": "Rate limit exceeded"}), 429

@app.route('/products', methods=['GET'])
def get_products():
    """Returns a list of products."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = "SELECT rowid, product_id, product_name, category_id FROM products WHERE 1=1"
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'products': [{'rowid': p[0],'product_id': p[1], 'product_name': p[2], 'category': p[3]} for p in rows],
        'next_cursor': next_cursor
    })

@app.route('/sales', methods=['GET'])
def get_sales():
    """Returns a list of sales."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = "SELECT rowid, sale_id, product_id, sale_date, quantity, price, category_id FROM sales WHERE 1=1"
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'sales': [{'rowid': s[0],'sale_id': s[1], 'product_id': s[2], 'sale_date': s[3], 'quantity': s[4], 'price': s[5], 'category':s[6]} for s in rows],
        'next_cursor': next_cursor
    })

@app.route('/categories', methods=['GET'])
def get_categories():
    """Returns a list of unique categories."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query =  "SELECT category_id, category_name FROM categories products WHERE 1=1 GROUP BY category" 
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'categories': [{ 'category_id': c[0],'category': c[1]} for c in rows],
        'next_cursor': next_cursor
    })

@app.route('/sales/daily_count', methods=['GET'])
def get_daily_sales_count():
    """Returns the count of sales per day."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = "SELECT rowid, sale_date, COUNT(*) FROM sales WHERE 1=1 GROUP BY sale_date"
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'daily_sales_count': [{ 'sale_date': c[1], 'count': c[2]} for c in rows],
        'next_cursor': next_cursor
    })

@app.route('/sales/product_daily_count', methods=['GET'])
def get_product_daily_sales_count():
    """Returns the count of sales per product per day."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = "SELECT rowid, sale_date, product_id, COUNT(*) FROM sales WHERE 1=1 GROUP BY sale_date, product_id "
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'product_daily_sales_count': [{'sale_date': c[1], 'product_id': c[2], 'count': c[3]} for c in rows],
        'next_cursor': next_cursor
    })

@app.route('/sales/category_sales', methods=['GET'])
def get_category_sales():
    """Returns the total sales amount and count per category."""
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = """
        SELECT c.category_name, SUM(s.price * s.quantity), COUNT(s.sale_id)
        FROM sales s
        JOIN categories c ON s.category_id = c.category_id
        GROUP BY c.category_name
    """
    rows, next_cursor = fetch_paginated_data(query, [], limit, cursor_val)
    return jsonify({
        'category_sales': [{'category': s[0], 'total_sales': s[1], 'total_count': s[2]} for s in rows],
        'next_cursor': next_cursor
    })

@app.route('/sales/filtered', methods=['GET'])
def get_filtered_sales():
    """Returns filtered sales based on date and/or product and/or."""
    date_filter = request.args.get('date')
    category_filter = request.args.get('category')
    product_filter = request.args.get('product_id')
    limit = int(request.args.get('limit', 10))
    cursor_val = request.args.get('cursor')
    query = "SELECT rowid, sale_id, product_id, sale_date, quantity, price, category_id FROM sales WHERE 1=1"
    params = []

    if date_filter:
        query += " AND sale_date LIKE ?"
        params.append(f"{date_filter}%")
    if product_filter:
        query += " AND product_id = ?"
        params.append(product_filter)
    if category_filter:
        query += " AND category_id = ?"
        params.append(category_filter)

    rows, next_cursor = fetch_paginated_data(query, params, limit, cursor_val)
    return jsonify({
        'filtered_sales': [{'rowid': s[0], 'sale_id': s[1], 'product_id': s[2], 'sale_date': s[3], 'quantity': s[4], 'price': s[5], 'category':s[6]} for s in rows],
        'next_cursor': next_cursor
    })

@app.route('/help', methods=['GET'])
def help_route():
    """Returns a list of available routes and their usage."""
    routes = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint != 'static':  
            methods = ', '.join(rule.methods)
            docstring = app.view_functions[rule.endpoint].__doc__ or "No documentation available."
            routes.append({
                'endpoint': rule.endpoint,
                'methods': methods,
                'route': str(rule),
                'description': docstring.strip()
            })
    return jsonify({'routes': routes})


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)