import os
import ldap
import ldap.modlist as modlist
from logger_config import setup_logging

# Get the configured logger
logger = setup_logging()

# Load AD config from environment variables
AD_SERVER = os.getenv('AD_SERVER')
AD_PORT = 389
AD_BIND_USER = os.getenv('AD_BIND_USER')
AD_BIND_PASS = os.getenv('AD_BIND_PASSWORD')
AD_BASE_DN = os.getenv('AD_BASE_DN')
AD_DOMAIN_NAME = os.getenv('AD_DOMAIN_NAME')

def get_ldap_connection():
    """Establishes and binds a connection to Active Directory."""
    conn = ldap.initialize(f"ldap://{AD_SERVER}:{AD_PORT}")
    conn.protocol_version = ldap.VERSION3
    conn.set_option(ldap.OPT_REFERRALS, 0)
    # The user for binding needs to be in a DN format, e.g., cn=svc_scim,ou=SCIM_Users,dc=poc,dc=local
    bind_dn = f"cn={AD_BIND_USER},{AD_BASE_DN}"
    conn.simple_bind_s(bind_dn, AD_BIND_PASS)
    return conn

def check_ad_connection():
    try:
        conn = get_ldap_connection()
        conn.unbind_s()
        return True, "Success"
    except ldap.LDAPError as e:
        logger.error("Active Directory connection check failed", extra={'error': str(e)})
        return False, str(e)

def _find_user_dn(conn, user_id):
    """Helper to find a user's full Distinguished Name by their username."""
    search_filter = f"(sAMAccountName={user_id})"
    results = conn.search_s(AD_BASE_DN, ldap.SCOPE_SUBTREE, search_filter, ['dn'])
    if not results:
        return None
    return results[0][0]

def create_user_in_ad(scim_data):
    """Creates a user in Active Directory from a SCIM data dictionary."""
    username = scim_data.get('userName')
    first_name = scim_data.get('name', {}).get('givenName')
    last_name = scim_data.get('name', {}).get('familyName')
    display_name = scim_data.get('displayName')
    password = scim_data.get('password')

    if not all([username, first_name, last_name, display_name, password]):
        logger.error("Create user failed due to missing attributes", extra={'user': username})
        raise ValueError("Missing required user attributes in SCIM data.")

    user_dn = f"CN={display_name},{AD_BASE_DN}"
    attrs = {
        'objectClass': [b'top', b'person', b'organizationalPerson', b'user'],
        'cn': [display_name.encode('utf-8')],
        'sAMAccountName': [username.encode('utf-8')],
        'givenName': [first_name.encode('utf-8')],
        'sn': [last_name.encode('utf-8')],
        'userPrincipalName': [f"{username}@{AD_DOMAIN_NAME}".encode('utf-8')],
        'userAccountControl': [b'544']  # 544 = Disabled, password not required
    }
    
    conn = None
    try:
        conn = get_ldap_connection()
        conn.add_s(user_dn, modlist.addModlist(attrs))
        
        encoded_password = f'"{password}"'.encode('utf-16-le')
        conn.modify_s(user_dn, [(ldap.MOD_REPLACE, 'unicodePwd', [encoded_password])])

        # 512 = Normal Enabled Account
        conn.modify_s(user_dn, [(ldap.MOD_REPLACE, 'userAccountControl', [b'512'])])
        logger.info("Successfully created user in AD", extra={'user': username, 'dn': user_dn})
    except ldap.LDAPError as e:
        logger.error("Failed to create user in AD", extra={'user': username, 'error': str(e)})
        raise e  # Re-raise exception for RQ to handle the failure
    finally:
        if conn:
            conn.unbind_s()

def read_user_from_ad(user_id):
    """Reads a user from AD and formats the response for SCIM."""
    conn = get_ldap_connection()
    try:
        user_dn = _find_user_dn(conn, user_id)
        if not user_dn:
            return False, "User not found"
        
        results = conn.search_s(user_dn, ldap.SCOPE_BASE)
        user_attrs = results[0][1]

        is_active = not (int(user_attrs.get('userAccountControl', [b'0'])[0]) & 2)
        
        scim_response = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "id": user_attrs['sAMAccountName'][0].decode('utf-8'),
            "userName": user_attrs['sAMAccountName'][0].decode('utf-8'),
            "active": is_active,
            "name": {
                "givenName": user_attrs.get('givenName', [b''])[0].decode('utf-8'),
                "familyName": user_attrs.get('sn', [b''])[0].decode('utf-8')
            }
        }
        return True, scim_response
    except ldap.LDAPError as e:
        logger.error("Failed to read user from AD", extra={'user': user_id, 'error': str(e)})
        return False, str(e)
    finally:
        conn.unbind_s()

def update_user_in_ad(user_id, patch_data):
    """Updates a user based on a SCIM PATCH request."""
    conn = get_ldap_connection()
    try:
        user_dn = _find_user_dn(conn, user_id)
        if not user_dn:
            raise ValueError(f"User {user_id} not found for update.")
        
        if patch_data.get('active') is False:
            conn.modify_s(user_dn, [(ldap.MOD_REPLACE, 'userAccountControl', [b'514'])]) # 514 = Disabled
            logger.info("Successfully deactivated user in AD", extra={'user': user_id})
        
        if 'password' in patch_data:
            new_password = patch_data['password']
            encoded_password = f'"{new_password}"'.encode('utf-16-le')
            conn.modify_s(user_dn, [(ldap.MOD_REPLACE, 'unicodePwd', [encoded_password])])
            logger.info("Successfully updated password for user in AD", extra={'user': user_id})
            
    except ldap.LDAPError as e:
        logger.error("Failed to update user in AD", extra={'user': user_id, 'error': str(e)})
        raise e
    finally:
        conn.unbind_s()

def delete_user_in_ad(user_id):
    """Deletes a user from Active Directory."""
    conn = get_ldap_connection()
    try:
        user_dn = _find_user_dn(conn, user_id)
        if not user_dn:
            logger.info("User not found for deletion, skipping.", extra={'user': user_id})
            return
        
        conn.delete_s(user_dn)
        logger.info("Successfully deleted user from AD", extra={'user': user_id})
    except ldap.LDAPError as e:
        logger.error("Failed to delete user from AD", extra={'user': user_id, 'error': str(e)})
        raise e
    finally:
        conn.unbind_s()