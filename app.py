import os
from flask import Flask, request, jsonify
from waitress import serve
from dotenv import load_dotenv
from logger_config import setup_logging

# Setup Shared Logger
logger = setup_logging()

# Load environment variables and adapter functions
load_dotenv()
from adapter import (
    create_user_in_ad, 
    read_user_from_ad,
    update_user_in_ad, 
    delete_user_in_ad,
    check_ad_connection
)

# App & Queue Setup
app = Flask(__name__)
SCIM_API_TOKEN = os.getenv('SCIM_API_TOKEN')

@app.before_request
def auth_and_log():
    """Middleware to authenticate and log every SCIM request."""
    if request.path.startswith('/scim/v2'):
        if request.headers.get('Authorization') != f"Bearer {SCIM_API_TOKEN}":
            logger.warning("Authentication failed", extra={'remote_addr': request.remote_addr})
            return jsonify({"error": "Unauthorized"}), 401
    logger.info("Request received", extra={'method': request.method, 'path': request.path})

@app.route('/scim/v2/Users', methods=['POST'])
def scim_create_user():
    user_data = request.get_json()
    q.enqueue(create_user_in_ad, user_data, retry=retry_policy)
    logger.info("Create user job enqueued", extra={'userName': user_data.get('userName')})
    return jsonify(user_data), 201

@app.route('/scim/v2/Users/<user_id>', methods=['GET'])
def scim_read_user(user_id):
    success, result = read_user_from_ad(user_id)
    if success:
        return jsonify(result), 200
    return jsonify({"error": result}), 404

@app.route('/scim/v2/Users/<user_id>', methods=['PATCH'])
def scim_update_user(user_id):
    patch_data = request.get_json()
    q.enqueue(update_user_in_ad, user_id, patch_data, retry=retry_policy)
    logger.info("Update user job enqueued", extra={'user_id': user_id})
    return jsonify({}), 200

@app.route('/scim/v2/Users/<user_id>', methods=['DELETE'])
def scim_delete_user(user_id):
    q.enqueue(delete_user_in_ad, user_id, retry=retry_policy)
    logger.info("Delete user job enqueued", extra={'user_id': user_id})
    return "", 204

@app.route('/scim/v2/Bulk', methods=['POST'])
def scim_bulk_operations():
    bulk_data = request.get_json()
    operations = bulk_data.get('Operations', [])
    logger.info(f"Received bulk request with {len(operations)} operations.")

    for op in operations:
        method = op.get('method', '').upper()
        path = op.get('path', '')
        data = op.get('data', {})
        
        if method == 'POST' and '/Users' in path:
            q.enqueue(create_user_in_ad, data, retry=retry_policy)
        elif method == 'PATCH' and '/Users/' in path:
            user_id = path.split('/')[-1]
            q.enqueue(update_user_in_ad, user_id, data, retry=retry_policy)
        elif method == 'DELETE' and '/Users/' in path:
            user_id = path.split('/')[-1]
            q.enqueue(delete_user_in_ad, user_id, retry=retry_policy)
            
    return jsonify({"status": "Bulk request accepted"}), 200
    
@app.route('/health', methods=['GET'])
def health_check():
    redis_ok = False
    try:
        redis_conn.ping()
        redis_ok = True
    except RedisConnectionError:
        pass
    ad_ok, _ = check_ad_connection()
    status = {
        'redis_status': 'ok' if redis_ok else 'error',
        'ad_status': 'ok' if ad_ok else 'error'
    }
    http_code = 200 if redis_ok and ad_ok else 503
    return jsonify(status), http_code

if __name__ == '__main__':
    logger.info("Starting SCIM Connector API server.")
    serve(app, host='0.0.0.0', port=5000)