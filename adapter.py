import subprocess
import shlex
from logger_config import setup_logging

# Setup logger
logger = setup_logging()

def run_kadmin_command(command):
    """Run a kadmin.local command and return output or raise error."""
    try:
        full_cmd = f'kadmin.local -q "{command}"'
        result = subprocess.run(
            shlex.split(full_cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=False
        )
        if result.returncode != 0:
            raise Exception(result.stderr.strip())
        return result.stdout.strip()
    except Exception as e:
        logger.error("kadmin.local command failed", extra={'command': command, 'error': str(e)})
        raise e

def check_ad_connection():
    """Dummy health check using listprincs."""
    try:
        output = run_kadmin_command("listprincs")
        return True, "Success"
    except Exception as e:
        return False, str(e)

def create_user_in_ad(scim_data):
    """Create a new Kerberos principal."""
    username = scim_data.get('userName')
    password = scim_data.get('password')

    if not username or not password:
        logger.error("Missing required SCIM user fields", extra={'user': username})
        raise ValueError("Missing required user attributes")

    try:
        output = run_kadmin_command(f"addprinc -pw {password} {username}")
        logger.info("Kerberos principal created", extra={'user': username, 'output': output})
    except Exception as e:
        logger.error("Failed to create principal", extra={'user': username, 'error': str(e)})
        raise e

def read_user_from_ad(user_id):
    """Read Kerberos principal details."""
    try:
        output = run_kadmin_command(f"getprinc {user_id}")
        if "Principal:" not in output:
            return False, "User not found"

        scim_response = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": user_id,
            "userName": user_id,
            "active": True,  # Kerberos doesn't support "inactive", always return True
            "name": {
                "givenName": "",
                "familyName": ""
            }
        }
        return True, scim_response
    except Exception as e:
        logger.error("Failed to read principal", extra={'user': user_id, 'error': str(e)})
        return False, str(e)

def update_user_in_ad(user_id, patch_data):
    """Update a Kerberos principal. Currently only supports password change."""
    try:
        if 'password' in patch_data:
            new_password = patch_data['password']
            output = run_kadmin_command(f"cpw -pw {new_password} {user_id}")
            logger.info("Password updated for principal", extra={'user': user_id, 'output': output})

        if patch_data.get('active') is False:
            logger.warning("Kerberos does not support disabling users. Skipping.", extra={'user': user_id})

    except Exception as e:
        logger.error("Failed to update principal", extra={'user': user_id, 'error': str(e)})
        raise e

def delete_user_in_ad(user_id):
    """Delete a Kerberos principal."""
    try:
        output = run_kadmin_command(f"delprinc -force {user_id}")
        logger.info("Kerberos principal deleted", extra={'user': user_id, 'output': output})
    except Exception as e:
        logger.error("Failed to delete principal", extra={'user': user_id, 'error': str(e)})
        raise e
