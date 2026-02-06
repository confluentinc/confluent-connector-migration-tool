"""
Shared utility functions for Confluent connector migration tools.
"""

import os
import json
import getpass
from datetime import datetime
import requests

# Constants
BASE_URL = "https://confluent.cloud/"
SCRUBBED_PASSWORD_STRING = "****************"


class APIError(Exception):
    """Custom exception for API errors."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


# Global state for token management
_auth_state = {
    'auth_token': None,
    'last_poll_time': datetime.now(),
    'user_email': None,
    'user_password': None
}


def get_credentials_input():
    """Handle credentials input with file support."""
    print("\n" + "="*60)
    print("Confluent Cloud Credentials")
    print("="*60)
    print("Choose how you want to provide your credentials:")
    print("1. Environment variables - Set EMAIL and PASSWORD environment variables")
    print("2. File - Provide path to a JSON file containing credentials (RECOMMENDED)")
    print("3. Secure input - Enter credentials manually (password hidden)")
    print()
    print("SECURITY NOTE: Option 2 (file) is recommended to avoid password exposure in command history.")

    cred_choice = input("Choose option (1-3, default is 2): ").strip()

    if cred_choice == "2" or not cred_choice:
        # Option 2: File (RECOMMENDED)
        while True:
            cred_file_path = input("Enter the path to your credentials JSON file: ").strip()
            if cred_file_path and os.path.exists(cred_file_path):
                try:
                    with open(cred_file_path, 'r') as f:
                        cred_data = json.load(f)

                    email = cred_data.get('email')
                    password = cred_data.get('password')

                    if email and password:
                        print(f"Credentials loaded from: {cred_file_path}")
                        return email, password
                    else:
                        print("Invalid credentials file format. Expected: {\"email\": \"...\", \"password\": \"...\"}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            return get_credentials_secure_input()
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON format in credentials file: {e}")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        return get_credentials_secure_input()
                except Exception as e:
                    print(f"Error reading credentials file: {e}")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        return get_credentials_secure_input()
            else:
                print("File not found. Please provide a valid file path.")
                retry = input("Try again? (yes/no): ").strip().lower()
                if retry not in ['yes', 'y']:
                    return get_credentials_secure_input()
    elif cred_choice == "3":
        # Option 3: Secure input
        return get_credentials_secure_input()
    else:
        # Option 1: Environment variables
        email = os.environ.get("EMAIL")
        password = os.environ.get("PASSWORD")

        if email and password:
            print("Credentials loaded from environment variables")
            print("NOTE: Environment variables may be visible in process lists and command history.")
            return email, password
        else:
            print("EMAIL and PASSWORD environment variables not set")
            print("Falling back to secure input...")
            return get_credentials_secure_input()


def get_credentials_secure_input():
    """Get credentials through secure user input (password hidden)."""
    print("\nSecure Credentials Input")
    print("Your password will be hidden when typing.")

    email = input("Enter your Confluent Cloud email: ").strip()

    # Use getpass for secure password input (hidden)
    password = getpass.getpass("Enter your Confluent Cloud password: ")

    if email and password:
        print("Credentials received securely")
        return email, password
    else:
        print("Email and password cannot be empty")
        return get_credentials_secure_input()


def get_auth_token(base_url, email=None, password=None):
    """Get/refresh authentication token."""
    global _auth_state

    # Use provided credentials or get them from state
    if email and password:
        _auth_state['user_email'] = email
        _auth_state['user_password'] = password

    if not _auth_state['user_email'] or not _auth_state['user_password']:
        raise APIError("Email or password not provided")

    url = base_url + "api/sessions"

    json_data = {
        'email': _auth_state['user_email'],
        'password': _auth_state['user_password']
    }

    response = requests.post(url, json=json_data)

    if not response.ok:
        raise APIError(f"Failed to get auth token: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        token = response.json().get('token')
        if not token:
            raise APIError("Auth token not found in response")
        _auth_state['auth_token'] = token
        _auth_state['last_poll_time'] = datetime.now()
        return token
    except json.JSONDecodeError:
        raise APIError("Failed to decode JSON while getting auth token", response_text=response.text)


def _refresh_token_if_needed(base_url):
    """Refresh auth token if it's been more than 3 minutes since last poll."""
    if (datetime.now() - _auth_state['last_poll_time']).total_seconds() > 180:
        get_auth_token(base_url, _auth_state['user_email'], _auth_state['user_password'])


def get_connector_config(base_url, env, lkc, connector_name):
    """Fetch connector configuration."""
    _refresh_token_if_needed(base_url)

    cookies = {'auth_token': _auth_state['auth_token']}
    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}"

    response = requests.get(url, cookies=cookies)

    if not response.ok:
        raise APIError(f"Failed to get connector config for {connector_name}: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        json_response = response.json()
        return json_response["config"]
    except json.JSONDecodeError:
        raise APIError(f"Failed to decode JSON for connector config: {connector_name}", response_text=response.text)


def get_connector_offsets(base_url, env, lkc, connector_name):
    """Fetch connector offsets."""
    _refresh_token_if_needed(base_url)

    cookies = {'auth_token': _auth_state['auth_token']}
    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}/offsets"

    response = requests.get(url, cookies=cookies)

    if response.status_code == 404:
        # No offsets yet - connector hasn't committed
        return []

    if not response.ok:
        raise APIError(f"Failed to get connector offsets for {connector_name}: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        json_response = response.json()
        return json_response.get("offsets", [])
    except json.JSONDecodeError:
        raise APIError(f"Failed to decode JSON for connector offsets: {connector_name}", response_text=response.text)


def get_connector_status(base_url, env, lkc, connector_name):
    """Fetch connector status."""
    _refresh_token_if_needed(base_url)

    cookies = {'auth_token': _auth_state['auth_token']}
    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}/status"

    response = requests.get(url, cookies=cookies)

    if not response.ok:
        raise APIError(f"Failed to get connector status for {connector_name}: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        json_response = response.json()
        return json_response["connector"]["state"]
    except json.JSONDecodeError:
        raise APIError(f"Failed to decode JSON for connector status: {connector_name}", response_text=response.text)


def send_create_request(base_url, env, lkc, connector_name, config, offsets):
    """Create new connector with config and offsets."""
    _refresh_token_if_needed(base_url)

    cookies = {
        'auth_token': _auth_state['auth_token'],
    }

    new_connector_name = config.get("name", connector_name)

    json_data = {
        'name': new_connector_name,
        'config': config,
        'offsets': offsets
    }

    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors"

    response = requests.post(
        url,
        cookies=cookies,
        json=json_data,
    )

    if response.status_code != 201:
        raise APIError(f"Failed to create connector: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        json_response = response.json()
        print(f"Connector '{new_connector_name}' created successfully. Response: {json.dumps(json_response, indent=2)}")
        return json_response
    except json.JSONDecodeError:
        raise APIError(f"Failed to decode JSON response for connector creation", response_text=response.text)


def prompt_for_sensitive_values(config, scrubbed_string=SCRUBBED_PASSWORD_STRING, skip_keys=None):
    """Prompt user for any masked sensitive values."""
    if skip_keys is None:
        skip_keys = []

    for key, value in config.items():
        if value == scrubbed_string and key not in skip_keys:
            while True:
                user_input = getpass.getpass(f"Please enter the value for {key}: ")
                if user_input:
                    config[key] = user_input
                    break
                else:
                    print("Input cannot be empty. Please try again.")

    return config


def display_config_and_confirm(config, message="Proceed with creating the connector?", mask_keys=None):
    """Display config JSON and get user confirmation."""
    if mask_keys is None:
        mask_keys = []

    print("\n" + "="*80)
    print("FINAL CONNECTOR CONFIGURATION")
    print("="*80)

    # Mask sensitive values for display
    display_config = config.copy()
    for key in mask_keys:
        if key in display_config:
            display_config[key] = '********'

    print(json.dumps(display_config, indent=4))
    print("="*80)

    user_input = input(f"\nPlease review the above configuration. {message} (yes/no): ")
    return user_input.lower() == 'yes'


def initialize_auth(base_url):
    """Initialize authentication by getting credentials and auth token."""
    print("Setting up Confluent Cloud authentication...")
    email, password = get_credentials_input()
    get_auth_token(base_url, email, password)
    return email, password


def check_connector_status_and_confirm(status, connector_name):
    """
    Check connector status and get user confirmation if connector is running.

    Returns:
        True if migration should proceed, False otherwise.
    """
    if status == "RUNNING":
        print("\n" + "="*80)
        print("CONNECTOR STATUS WARNING")
        print("="*80)
        print(f"Your connector '{connector_name}' is currently RUNNING.")
        print()
        print("  * If you are testing on dummy resources, you can keep the connector running")
        print("  * For migrating production data, it is recommended to PAUSE the connector")
        print("    to avoid data duplication")
        print()
        print("The migration will proceed, but be aware of potential data duplication.")
        print("="*80)

        user_input = input("Do you still want to proceed? (yes/no): ")
        if user_input.lower() != 'yes':
            print("Exiting the migration tool...")
            return False
    elif status == "PAUSED":
        print(f"Connector '{connector_name}' is paused - safe to proceed with migration")
    else:
        print(f"Connector status: {status}")

    return True
