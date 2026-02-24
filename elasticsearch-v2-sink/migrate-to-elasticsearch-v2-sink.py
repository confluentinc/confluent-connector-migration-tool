#!/usr/bin/env python3
"""
Elasticsearch Sink V1 to V2 Migration Tool

This tool migrates Elasticsearch Sink V1 connectors to V2 connectors in Confluent Cloud.
V1 and V2 are architecturally different connectors - V1 uses Elasticsearch SDK while V2
uses a modern HTTP framework. Direct in-place upgrade is not possible, so this tool
creates a new V2 connector with preserved offsets.
"""

import argparse
import json
import getpass
import sys
import os
from datetime import datetime
import requests

# Constants
BASE_URL = "https://confluent.cloud/"
SCRUBBED_PASSWORD_STRING = "****************"

# Global variables for authentication state
auth_token = None
last_poll_time = datetime.now()
user_email = None
user_password = None


class APIError(Exception):
    """Custom exception for API errors."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


# ============================================================================
# Utility Functions
# ============================================================================

def get_auth_token(email=None, password=None):
    """Get/refresh authentication token."""
    global auth_token, last_poll_time, user_email, user_password

    # Store credentials on first call
    if email and password:
        user_email = email
        user_password = password

    # Ensure we have credentials
    if not user_email or not user_password:
        raise APIError("Email or password not provided")

    url = BASE_URL + "api/sessions"
    json_data = {
        'email': user_email,
        'password': user_password
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
        auth_token = token
        last_poll_time = datetime.now()
        return token
    except json.JSONDecodeError:
        raise APIError("Failed to decode JSON while getting auth token", response_text=response.text)


def refresh_token_if_needed():
    """Refresh auth token if it's been more than 3 minutes since last poll."""
    global last_poll_time

    if (datetime.now() - last_poll_time).total_seconds() > 180:
        get_auth_token(user_email, user_password)


def get_connector_config(env, lkc, connector_name):
    """Fetch connector configuration."""
    refresh_token_if_needed()

    cookies = {'auth_token': auth_token}
    url = f"{BASE_URL}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}"

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


def get_connector_offsets(env, lkc, connector_name):
    """Fetch connector offsets."""
    refresh_token_if_needed()

    cookies = {'auth_token': auth_token}
    url = f"{BASE_URL}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}/offsets"

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


def get_connector_status(env, lkc, connector_name):
    """Fetch connector status."""
    refresh_token_if_needed()

    cookies = {'auth_token': auth_token}
    url = f"{BASE_URL}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}/status"

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


def send_create_request(env, lkc, connector_name, config, offsets):
    """Create new connector with config and offsets."""
    refresh_token_if_needed()

    cookies = {'auth_token': auth_token}
    new_connector_name = config.get("name", connector_name)

    json_data = {
        'name': new_connector_name,
        'config': config,
        'offsets': offsets
    }

    url = f"{BASE_URL}api/accounts/{env}/clusters/{lkc}/connectors"

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


# ============================================================================
# Configuration Mappings
# ============================================================================

# Renamed mappings (V1 name -> V2 name)
RENAMED_MAPPING = {
    "topic.to.external.resource.mapping": "topic.to.resource.mapping",
}

# Discontinued properties - drop with warning
DISCONTINUED = {
    "drop.invalid.message": "Not directly supported in V2. Task will fail on preprocessing errors.",
    "linger.ms": "Removed. V2 uses framework-level batching.",
    "flush.timeout.ms": "Removed. V2 uses framework-level timeout.",
    "external.resource.usage": "Split into 'auto.create' + 'resource.type' in V2 (derived automatically during migration).",
}

# ============================================================================
# Breaking Changes
# ============================================================================

BREAKING_CHANGES = {
    "CONNECTION_URL": "V1 supported multiple URLs (comma-separated), V2 supports only a single URL. If multiple URLs are configured, you will be prompted to enter which URL to use.",
    "DELETE_HANDLING": "V2 requires a valid document '_id' for DELETE operations. This only applies when 'key.ignore=false'. Ensure your records have proper keys when using 'behavior.on.null.values=delete'.",
    "ERROR_ROUTING": "V2 uses error topics instead of DLQ for error handling. Review your error handling strategy after migration.",
    "BEHAVIOR_ON_MALFORMED_DOCS": "V2 only supports 'ignore' and 'fail' for behavior.on.malformed.documents. 'warn' is not supported and will be converted to 'ignore'.",
}


# ============================================================================
# Helper Functions
# ============================================================================

def show_breaking_changes_warning():
    """Display breaking changes warning to the user."""
    print("\n" + "="*80)
    print("IMPORTANT: BREAKING API CHANGES")
    print("="*80)
    print("The Elasticsearch V2 connector has architectural differences from V1:")
    print()

    for change_type, description in BREAKING_CHANGES.items():
        print(f"  * {change_type}: {description}")

    print("\n" + "-"*80)
    print("RECOMMENDATIONS:")
    print("1. Test the migration with a non-production connector first")
    print("2. Verify data integrity after migration")
    print("3. Review V2 documentation for any additional changes")
    print("-"*80)

    user_input = input("\nDo you understand these breaking changes and want to proceed? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled due to breaking changes concerns.")
        return False
    return True


def check_ssl_file_configs(v1_config):
    """Check for SSL file configurations that need manual upload."""
    ssl_file_configs = {
        "elastic.https.ssl.truststore.file": "Truststore with CA certificates",
        "elastic.https.ssl.keystore.file": "Keystore with client certificates"
    }

    found_ssl_files = []
    for config_key, description in ssl_file_configs.items():
        if config_key in v1_config and v1_config[config_key]:
            found_ssl_files.append((config_key, description))

    return found_ssl_files


def show_ssl_file_warning(ssl_file_configs):
    """Display warning about SSL file configurations."""
    if not ssl_file_configs:
        return True

    print("\n" + "="*80)
    print("WARNING: SSL FILE CONFIGURATIONS DETECTED")
    print("="*80)
    print("The following SSL certificate files are configured in your V1 connector:")
    print()

    for config_key, description in ssl_file_configs:
        print(f"  * {config_key}")
        print(f"    -> {description}")
        print()

    print("-"*80)
    print("IMPORTANT: SSL certificate files are NOT automatically migrated!")
    print()
    print("After creating the V2 connector, you MUST:")
    print("  1. Go to Confluent Cloud Console")
    print("  2. Navigate to Connectors > Your V2 Connector > Settings")
    print("  3. Manually upload the keystore/truststore files")
    print("  4. Resume the connector after uploading files")
    print()
    print("Without these files, SSL/TLS connections will fail.")
    print("-"*80)

    user_input = input("\nDo you understand that SSL files need manual upload after migration? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled.")
        return False
    return True


def check_discontinued_configs(v1_config):
    """Check for configurations that are not supported in V2 connector."""
    found_discontinued = []

    for config_key in DISCONTINUED.keys():
        if config_key in v1_config:
            found_discontinued.append(config_key)

    return found_discontinued


def show_discontinued_configs_warning(discontinued_configs, v1_config):
    """Display warning about discontinued configurations."""
    if not discontinued_configs:
        return True

    print("\n" + "="*80)
    print("DISCONTINUED CONFIGURATIONS DETECTED")
    print("="*80)
    print("The following configurations are NOT SUPPORTED in V2 connector:")
    print()

    for config_key in discontinued_configs:
        current_value = v1_config.get(config_key, "N/A")
        print(f"  * {config_key}: {current_value}")
        print(f"    -> {DISCONTINUED[config_key]}")
        print()

    print("-"*80)
    print("IMPACT: These configurations will be dropped during migration.")
    print("-"*80)

    user_input = input("\nDo you understand that these configurations will not be migrated? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled.")
        return False
    return True


def derive_v2_properties(v1_config):
    """Derive V2 properties from V1 configuration."""

    # elastic.ssl.enabled: derived from elastic.security.protocol
    # Empty or not present or "PLAINTEXT" means SSL is disabled
    security_protocol = v1_config.get("elastic.security.protocol", "")
    ssl_enabled = security_protocol.upper() == "SSL"

    # auto.create & resource.type: derived from external.resource.usage / data.stream.type
    # Empty or not present or "DISABLED" means auto-create is enabled
    external_usage = v1_config.get("external.resource.usage", "")

    if not external_usage or external_usage.upper() == "DISABLED":
        # V1 auto-creates resources -> V2 auto.create = True
        auto_create = True
        # Derive resource_type from data.stream.type
        ds_type = v1_config.get("data.stream.type", "")
        if ds_type and ds_type.upper() not in ["NONE", ""]:
            resource_type = "DATASTREAM"
        else:
            resource_type = "INDEX"
    else:
        # V1 uses pre-created resources -> V2 auto.create = False
        auto_create = False
        resource_type = external_usage.upper()  # INDEX, DATASTREAM, ALIAS_INDEX, ALIAS_DATASTREAM

    return {
        "ssl_enabled": ssl_enabled,
        "auto_create": auto_create,
        "resource_type": resource_type,
    }


def ask_migration_mode():
    """
    Ask user whether to run in test mode or production mode.

    Returns:
        str: 'test' or 'production'
    """
    print("\n" + "="*60)
    print("Migration Mode Selection")
    print("="*60)
    print("Choose migration mode:")
    print()
    print("  1. PRODUCTION - Standard migration (recommended for final migration)")
    print("     * Your new V2 connector will have similar configuration to the V1 connector")
    print()
    print("  2. TEST - Create test connector to validate before final migration")
    print("     * You must provide topic-to-resource mapping")
    print("     * Allows testing with subset of topics")
    print()

    while True:
        choice = input("Choose mode (1-2, default is 1): ").strip()
        if choice == "" or choice == "1":
            print("Using PRODUCTION mode.")
            return 'production'
        elif choice == "2":
            print("Using TEST mode.")
            return 'test'
        else:
            print("Invalid choice. Please enter 1 or 2.")


def get_test_mode_configuration(v1_config):
    """
    Get configuration for test mode migration.

    In test mode:
    - auto.create is always false
    - User must provide topic.to.resource.mapping
    - Validates mapping topics match topics config
    - Allows updating topics config for subset testing

    Returns:
        dict: {
            'topics': str,
            'topic_to_resource_mapping': str
        }
    """
    print("\n" + "="*60)
    print("Test Mode Configuration")
    print("="*60)

    # Show current topics
    topics_str = v1_config.get("topics", "")
    print(f"Current topics: {topics_str if topics_str else '(not configured)'}")
    print()

    # Step 1: Ask if user wants to update topics (for testing subset)
    print("You can update the topics config to test with a subset of topics.")
    update_topics = input("Do you want to update the topics config? (yes/no, default is no): ").strip().lower()

    if update_topics in ['yes', 'y']:
        while True:
            new_topics = input("Enter topics (comma-separated): ").strip()
            if new_topics:
                topics_str = new_topics
                print(f"Topics updated to: {topics_str}")
                break
            print("Topics cannot be empty.")

    # Parse topics for validation
    topics_list = [t.strip() for t in topics_str.split(",") if t.strip()]

    # Step 2: Get topic-to-resource mapping
    print()
    print("Enter topic-to-resource mapping.")
    print("Format: topic1:resource1,topic2:resource2")
    print(f"Topics to map: {', '.join(topics_list)}")
    print()

    while True:
        mapping_input = input("Enter topic-to-resource mapping: ").strip()
        if not mapping_input:
            print("Mapping cannot be empty in test mode.")
            continue

        # Parse and validate mapping
        mapping_topics = set()
        valid_format = True
        for pair in mapping_input.split(","):
            pair = pair.strip()
            if ":" not in pair:
                print(f"Invalid format '{pair}'. Expected 'topic:resource'")
                valid_format = False
                break
            topic, _ = pair.split(":", 1)
            mapping_topics.add(topic.strip())

        if not valid_format:
            continue

        # Validate: mapping topics must match topics config exactly
        topics_set = set(topics_list)

        missing_in_mapping = topics_set - mapping_topics
        extra_in_mapping = mapping_topics - topics_set

        if missing_in_mapping:
            print(f"\nError: These topics are in 'topics' config but missing from mapping:")
            print(f"  {', '.join(missing_in_mapping)}")
            print("All topics must have a mapping. Please try again.")
            continue

        if extra_in_mapping:
            print(f"\nError: These topics are in mapping but not in 'topics' config:")
            print(f"  {', '.join(extra_in_mapping)}")
            print("Please try again with correct topics.")
            continue

        break

    print(f"\nTest mode configuration:")
    print(f"  * auto.create: false")
    print(f"  * topics: {topics_str}")
    print(f"  * topic.to.resource.mapping: {mapping_input}")

    return {
        'topics': topics_str,
        'topic_to_resource_mapping': mapping_input
    }


def apply_test_mode_overrides(v2_config, test_config):
    """
    Apply test mode overrides to V2 configuration.

    Overrides:
    - auto.create = false
    - topics = user-provided topics
    - topic.to.resource.mapping = user-provided mapping

    Args:
        v2_config: V2 configuration dict from transform_v1_to_v2()
        test_config: Test mode configuration from get_test_mode_configuration()

    Returns:
        tuple: (updated v2_config, list of override warnings)
    """
    warnings = []

    # Override auto.create
    v2_config["auto.create"] = "false"
    warnings.append("Test mode: auto.create set to 'false'")

    # Override topics
    v2_config["topics"] = test_config['topics']
    warnings.append(f"Test mode: topics set to '{test_config['topics']}'")

    # Override topic.to.resource.mapping
    v2_config["topic.to.resource.mapping"] = test_config['topic_to_resource_mapping']
    warnings.append(f"Test mode: topic.to.resource.mapping set to '{test_config['topic_to_resource_mapping']}'")

    return v2_config, warnings


def get_user_inputs(v1_config, derived, migration_mode='production'):
    """Collect user inputs for V2 configuration."""
    print("\n" + "="*60)
    print("MIGRATION CONFIGURATION")
    print("="*60)

    # 1. New connector name (based on migration mode)
    v1_name = v1_config.get('name', 'elasticsearch-sink')
    if migration_mode == 'test':
        default_name = f"{v1_name}-test-v2"
    else:
        default_name = f"{v1_name}-v2"
    while True:
        new_name = input(f"\nEnter new connector name (default: {default_name}): ").strip() or default_name
        if new_name != v1_config.get('name'):
            break
        else:
            print("New connector name must be different from the V1 connector name.")

    # 2. Elasticsearch server version (required - cannot derive from V1)
    print("\nElasticsearch Server Version:")
    print("  1. V8 (default)")
    print("  2. V9")
    while True:
        server_version_choice = input("Choose (1-2) [1]: ").strip()
        if server_version_choice == "" or server_version_choice == "1":
            server_version = "V8"
            break
        elif server_version_choice == "2":
            server_version = "V9"
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")
    print(f"Selected server version: {server_version}")

    # 3. Handle connection.url (V2 only supports single URL)
    connection_url = v1_config.get("connection.url", "")
    if "," in connection_url:
        print("\nConnection URL:")
        print(f"  V1 config has multiple URLs: {connection_url}")
        print("  V2 only supports a single URL. Please select or enter the URL to use.")

        # Parse the available URLs
        available_urls = [url.strip() for url in connection_url.split(",")]
        print("\nAvailable URLs:")
        for i, url in enumerate(available_urls, 1):
            print(f"  {i}. {url}")

        while True:
            user_input = input("\nEnter URL number (1-{}) or type a URL directly: ".format(len(available_urls))).strip()

            # Check if user entered a number
            try:
                url_index = int(user_input)
                if 1 <= url_index <= len(available_urls):
                    connection_url = available_urls[url_index - 1]
                    break
                else:
                    print(f"Invalid selection. Please enter a number between 1 and {len(available_urls)}.")
                    continue
            except ValueError:
                # User entered a URL directly
                connection_url = user_input

            if not connection_url:
                print("URL cannot be empty. Please try again.")
                continue
            elif "," in connection_url:
                print("V2 only supports a single URL. Please enter a single URL.")
                continue

            # Validate that the URL is reasonable (basic check)
            if not connection_url.startswith(("http://", "https://")):
                print("URL should start with http:// or https://. Please try again.")
                continue

            break

        print(f"Using connection URL: {connection_url}")

    # 4. Authentication type (ask user explicitly)
    print("\nAuthentication Type:")
    print("  1. BASIC   - Username & password authentication")
    print("                Requires: connection.username, connection.password")
    print("  2. API_KEY - API key authentication")
    print("                Requires: api.key.value")
    print("  3. NONE    - No authentication required")

    auth_type = None
    connection_username = None
    connection_password = None
    api_key_value = None

    while True:
        auth_choice = input("\nChoose (1-3) [1]: ").strip()
        if auth_choice == "" or auth_choice == "1":
            auth_type = "BASIC"
            connection_username = input("  Enter connection.username: ").strip()
            connection_password = getpass.getpass("  Enter connection.password: ")

            if not connection_username or not connection_password:
                print("ERROR: BASIC auth requires both username AND password!")
                continue
            break

        elif auth_choice == "2":
            auth_type = "API_KEY"
            api_key_value = getpass.getpass("  Enter api.key.value: ")

            if not api_key_value:
                print("ERROR: API_KEY auth requires api.key.value!")
                continue
            break

        elif auth_choice == "3":
            auth_type = "NONE"
            break

        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    print(f"\nSelected authentication type: {auth_type}")

    return {
        'new_connector_name': new_name,
        'server_version': server_version,
        'auth_type': auth_type,
        'connection_username': connection_username,
        'connection_password': connection_password,
        'api_key_value': api_key_value,
        'connection_url': connection_url,
    }


def transform_v1_to_v2(v1_config, user_inputs, derived):
    """Transform V1 configuration to V2 configuration."""
    warnings = []

    v2_config = {
        # V2 connector class
        "connector.class": "ElasticsearchSinkV2",

        # User inputs
        "name": user_inputs['new_connector_name'],
        "elastic.server.version": user_inputs['server_version'],
        "auth.type": user_inputs['auth_type'],

        # Derived from V1 config
        "elastic.ssl.enabled": str(derived['ssl_enabled']).lower(),
        "auto.create": str(derived['auto_create']).lower(),
        "resource.type": derived['resource_type'],
    }

    # Handle auth credentials based on auth.type
    if user_inputs['auth_type'] == "BASIC":
        v2_config["connection.username"] = user_inputs['connection_username']
        v2_config["connection.password"] = user_inputs['connection_password']
    elif user_inputs['auth_type'] == "API_KEY":
        v2_config["api.key.value"] = user_inputs['api_key_value']

    # Use connection.url from user_inputs (already validated for single URL)
    v2_config["connection.url"] = user_inputs['connection_url']

    # Apply renamed mappings
    for v1_key, v2_key in RENAMED_MAPPING.items():
        if v1_key in v1_config:
            # topic.to.resource.mapping only applies when auto.create=false
            if v2_key == "topic.to.resource.mapping" and derived['auto_create']:
                warnings.append(f"Dropping '{v1_key}' because auto.create=true (mapping only applies to pre-created resources)")
            else:
                v2_config[v2_key] = v1_config[v1_key]
                warnings.append(f"Renamed config: '{v1_key}' -> '{v2_key}'")

    # Copy all V1 configs to V2, except discontinued and specially handled ones
    # This includes: direct configs, SMT transforms, kafka.*, schema.registry.*, etc.
    excluded_keys = set(
        list(RENAMED_MAPPING.keys()) + list(DISCONTINUED.keys()) +
        ["name", "connector.class", "connection.url", "connection.username",
         "connection.password", "elastic.security.protocol", "external.resource.usage"]
    )

    for key, value in v1_config.items():
        if key not in excluded_keys and key not in v2_config:
            v2_config[key] = value

    # Handle data.stream.type: V1 default was NONE, V2 default is LOGS
    # If NONE, convert to LOGS; if empty, keep empty; otherwise keep as-is
    if "data.stream.type" in v2_config:
        ds_type = v2_config["data.stream.type"]
        if ds_type and ds_type.upper() == "NONE":
            v2_config["data.stream.type"] = "LOGS"
            warnings.append("Changed 'data.stream.type' from 'NONE' to 'LOGS'. 'NONE' is no longer supported in V2. 'LOGS' is the new default and only applies when resource.type is DATASTREAM. This will not cause any issues during migration.")

    # Handle behavior.on.malformed.documents: V2 only supports 'ignore' and 'fail'
    # 'warn' is not supported in V2, convert to 'ignore'
    if "behavior.on.malformed.documents" in v2_config:
        malformed_behavior = v2_config["behavior.on.malformed.documents"]
        if malformed_behavior and malformed_behavior.upper() == "WARN":
            v2_config["behavior.on.malformed.documents"] = "IGNORE"
            warnings.append("Changed 'behavior.on.malformed.documents' from 'warn' to 'ignore'. 'warn' is not supported in V2. Only 'ignore' and 'fail' are valid options.")

    return v2_config, warnings


def display_transformation_warnings(warnings):
    """Display any warnings from the transformation process."""
    if warnings:
        print("\n" + "-"*60)
        print("TRANSFORMATION NOTES:")
        print("-"*60)
        for warning in warnings:
            print(f"  * {warning}")
        print("-"*60)


# ============================================================================
# Main Migration Flow
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Migrate Elasticsearch Sink V1 connector to V2."
    )
    parser.add_argument('--v1_connector', required=True, help='Name of the V1 connector')
    parser.add_argument('--environment', required=True, help='Environment ID')
    parser.add_argument('--cluster_id', required=True, help='Cluster ID')
    args = parser.parse_args()

    connector_name = args.v1_connector
    env = args.environment
    lkc = args.cluster_id

    try:
        # Step 1: Show breaking changes warning
        print("\n" + "="*80)
        print("ELASTICSEARCH SINK V1 TO V2 MIGRATION TOOL")
        print("="*80)
        print(f"Migrating connector: {connector_name}")
        print(f"Environment: {env}")
        print(f"Cluster: {lkc}")

        if not show_breaking_changes_warning():
            return

        # Step 2: Get credentials and authenticate
        print("\nSetting up Confluent Cloud authentication...")
        email, password = get_credentials_input()
        get_auth_token(email, password)

        # Step 3: Check V1 connector status
        print("\nFetching V1 connector status...")
        status = get_connector_status(env, lkc, connector_name)

        if not check_connector_status_and_confirm(status, connector_name):
            return

        # Step 4: Fetch V1 config and offsets
        print("\nFetching V1 connector offsets...")
        offsets = get_connector_offsets(env, lkc, connector_name)
        print(f"Retrieved {len(offsets)} offset entries")

        print("Fetching V1 connector configuration...")
        v1_config = get_connector_config(env, lkc, connector_name)
        print(f"Retrieved {len(v1_config)} configuration properties")

        # Step 5: Check for SSL file configurations
        print("\nChecking for SSL file configurations...")
        ssl_file_configs = check_ssl_file_configs(v1_config)
        if ssl_file_configs:
            if not show_ssl_file_warning(ssl_file_configs):
                return

        # Step 6: Check for discontinued configs
        print("\nChecking for discontinued configurations...")
        discontinued_configs = check_discontinued_configs(v1_config)
        if discontinued_configs:
            if not show_discontinued_configs_warning(discontinued_configs, v1_config):
                return

        # Step 7: Choose migration mode
        migration_mode = ask_migration_mode()

        # Step 7b: Get test mode configuration if applicable
        test_config = None
        if migration_mode == 'test':
            test_config = get_test_mode_configuration(v1_config)

        # Step 8: Derive V2 properties
        print("\nDeriving V2 properties from V1 configuration...")
        derived = derive_v2_properties(v1_config)
        print(f"  - SSL enabled: {derived['ssl_enabled']}")
        print(f"  - Auto create: {derived['auto_create']}")
        print(f"  - Resource type: {derived['resource_type']}")

        # Step 9: Get user inputs for V2 properties
        user_inputs = get_user_inputs(v1_config, derived, migration_mode)

        # Step 10: Transform V1 -> V2 config
        print("\nTransforming V1 configuration to V2...")
        v2_config, warnings = transform_v1_to_v2(v1_config, user_inputs, derived)

        # Step 10b: Apply test mode overrides if applicable
        if test_config:
            v2_config, test_warnings = apply_test_mode_overrides(v2_config, test_config)
            warnings.extend(test_warnings)

        display_transformation_warnings(warnings)

        # Step 11: Prompt for any masked sensitive values
        v2_config = prompt_for_sensitive_values(
            v2_config,
            SCRUBBED_PASSWORD_STRING,
            skip_keys=["connection.password"]  # Already handled
        )

        # Step 12: Display final config for confirmation
        mask_keys = ["connection.password", "api.key.value", "kafka.api.secret",
                     "schema.registry.basic.auth.user.info"]

        if not display_config_and_confirm(v2_config,
                                           "Do you want to proceed with creating the V2 connector?",
                                           mask_keys=mask_keys):
            print("Migration cancelled.")
            return

        # Step 13: Create V2 connector with preserved offsets
        print("\nCreating V2 connector with preserved offsets...")
        send_create_request(env, lkc, user_inputs['new_connector_name'], v2_config, offsets)

        # Step 14: Show next steps
        print("\n" + "="*80)
        print("MIGRATION COMPLETED SUCCESSFULLY")
        print("="*80)
        print("\nNext steps:")
        print("  1. Verify the new V2 connector is running properly in Confluent Cloud Console")
        print("  2. Check that data is being written to Elasticsearch correctly")
        print("  3. Monitor connector metrics for any errors")
        print("  4. Once verified, you can delete the old V1 connector")
        print()
        print("IMPORTANT: The V2 connector starts from the preserved V1 offsets,")
        print("so no data should be duplicated or lost.")
        print("="*80)

    except APIError as e:
        print(f"\nAPI Error: {e}")
        if e.status_code:
            print(f"Status Code: {e.status_code}")
        if e.response_text:
            print(f"Response: {e.response_text}")
        sys.exit(1)
    except ValueError as e:
        print(f"\nValidation Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nMigration cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
