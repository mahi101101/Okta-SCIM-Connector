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
    try:
        create_user_in_ad(user_data)
        logger.info("User created", extra={'userName': user_data.get('userName')})
        return jsonify(user_data), 201
    except Exception as e:
        logger.error("Create user failed", extra={'error': str(e)})
        return jsonify({"error": str(e)}), 500


@app.route('/scim/v2/Users/<user_id>', methods=['GET'])
def scim_read_user(user_id):
    success, result = read_user_from_ad(user_id)
    if success:
        return jsonify(result), 200
    return jsonify({"error": result}), 404

@app.route('/scim/v2/Users/<user_id>', methods=['PATCH'])
def scim_update_user(user_id):
    patch_data = request.get_json()
    try:
        update_user_in_ad(user_id, patch_data)
        logger.info("User updated", extra={'user_id': user_id})
        return jsonify({}), 200
    except Exception as e:
        logger.error("Update failed", extra={'error': str(e)})
        return jsonify({"error": str(e)}), 500


@app.route('/scim/v2/Users/<user_id>', methods=['DELETE'])
def scim_delete_user(user_id):
    try:
        delete_user_in_ad(user_id)
        logger.info("User deleted", extra={'user_id': user_id})
        return "", 204
    except Exception as e:
        logger.error("Delete failed", extra={'error': str(e)})
        return jsonify({"error": str(e)}), 500

    
@app.route('/health', methods=['GET'])
def health_check():
    ad_ok, _ = check_ad_connection()
    status = {
        'ad_status': 'ok' if ad_ok else 'error'
    }
    return jsonify(status), 200 if ad_ok else 503


if __name__ == '__main__':
    logger.info("Starting SCIM Connector API server.")
    serve(app, host='0.0.0.0', port=5000)