import argparse
import os
import json
from datetime import datetime
import requests

auth_token = None
last_poll_time = datetime.now()
SCRAPPED_PASSWORD_STRING = "****************"

class APIError(Exception):
    """Custom exception for API errors."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

# Define the mapping between BigQuery Legacy and Storage Write API configurations
legacy_to_storage_mapping = {
    "keyfile": "keyfile",
    "project": "project",
    "datasets": "datasets",
    "topics": "topics",
    "tasks.max": "tasks.max",
    "sanitize.topics": "sanitize.topics",
    "sanitize.field.names": "sanitize.field.names",
    "auto.update.schemas": "auto.update.schemas",
    "input.data.format": "input.data.format",
    "input.key.format": "input.key.format",
    "table.name.format": "topic2table.map",
    "bigquery.retry.count": "bigQueryRetry",
    "bigquery.thread.pool.size": "threadPoolSize",
    "buffer.count.records": "queueSize"
}

# Default values for Storage Write API connector configurations
storage_defaults = {
    "sanitize.field.names": "false",
    "sanitize.field.names.in.array": "false",
    "auto.update.schemas": "DISABLED"
}

# Common configurations that should be preserved
common_configs = [
    "kafka.api.key",
    "kafka.api.secret",
    "kafka.service.account.id",
    "kafka.auth.mode",
    "kafka.endpoint",
    "kafka.region",
    "schema.registry.url",
    "schema.registry.basic.auth.user.info",
    "max.poll.interval.ms",
    "max.poll.records",
    "cloud.environment",
    "cloud.provider"
]

# Breaking changes warnings
BREAKING_CHANGES = {
    "TIMESTAMP": "TIMESTAMP values are now interpreted as microseconds since epoch instead of seconds. This may cause data to be written to incorrect time periods.",
    "DATE": "DATE fields now support INT values in range -719162 to 2932896, which was not supported in Legacy API.",
    "DATETIME_FORMAT": "DATE, TIME, DATETIME, TIMESTAMP fields now support only a subset of the datetime canonical format that was supported in Legacy API.",
    "DATA_TYPES": "Storage Write API has different data type support compared to Legacy InsertAll API. Some data types may not be compatible.",
    "INT8, INT16": "INT8 and INT16 fields are now cast to FLOAT type in BigQuery. You can choose to cast them to INTEGER instead in next steps."
}

# Configurations not supported in V2 Storage Write API connector
UNSUPPORTED_CONFIGS = {
    "allow.schema.unionization": "Schema unionization is not supported in V2 connector. This feature allowed combining record schemas with existing BigQuery table schemas.",
    "all.bq.fields.nullable": "All BigQuery fields nullable setting is not supported in V2 connector. This controlled whether all fields were made nullable.",
    "convert.double.special.values": "Double special values conversion is not supported in V2 connector. This handled +Infinity, -Infinity, and NaN conversions.",
    "allow.bigquery.required.field.relaxation": "BigQuery required field relaxation is not supported in V2 connector. This allowed relaxing required field constraints."
}

def show_breaking_changes_warning():
    """Display breaking changes warning to the user."""
    print("\n" + "="*80)
    print("⚠️  IMPORTANT: BREAKING API CHANGES")
    print("="*80)
    print("The BigQuery Storage Write API has breaking changes from the Legacy InsertAll API:")
    print()

    for change_type, description in BREAKING_CHANGES.items():
        print(f"• {change_type}: {description}")

    print("\n" + "-"*80)
    print("RECOMMENDATIONS:")
    print("1. Test the migration with a small dataset first")
    print("2. Verify data integrity after migration")
    print("3. Review documentation: https://docs.confluent.io/cloud/current/connectors/cc-gcp-bigquery-storage-sink.html#legacy-to-v2-connector-migration")
    print("-"*80)

    user_input = input("\nDo you understand these breaking changes and want to proceed? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled due to breaking changes concerns.")
        return False
    return True

def get_user_inputs(legacy_config):
    """Get user inputs for new connector configuration."""
    print("\n" + "="*60)
    print("MIGRATION CONFIGURATION")
    print("="*60)

    # Get new connector name
    while True:
        new_connector_name = input(f"\nEnter new connector name (default: {legacy_config['name']}-v2): ").strip()
        if not new_connector_name:
            new_connector_name = f"{legacy_config['name']}-v2"
        if new_connector_name != legacy_config['name']:
            break
        else:
            print("❌ New connector name must be different from the legacy connector name.")

    # Get ingestion mode with numbered options
    print("\n📊 Ingestion Mode Selection:")
    print("1. STREAMING - Lower latency, higher cost")
    print("2. BATCH LOADING - Higher latency, lower cost")
    print("3. UPSERT - For upsert operations")
    print("4. UPSERT_DELETE - For upsert and delete operations")

    mode_choice = input("Choose ingestion mode (1-4, default is 1 for STREAMING): ").strip()
    if mode_choice == "2":
        ingestion_mode = "BATCH LOADING"
    elif mode_choice == "3":
        ingestion_mode = "UPSERT"
        print("DISCLAIMER: For UPSERT mode, the records must have key fields.")
    elif mode_choice == "4":
        ingestion_mode = "UPSERT_DELETE"
        print("DISCLAIMER: For UPSERT_DELETE mode, the records must have key fields.")
    else:
        ingestion_mode = "STREAMING"

    # Get Int8/Int16 type casting preference
    print("\n🔢 Int8/Int16 Type Casting:")
    print("In the new Storage Write API connector, INT8 (BYTE) and INT16 (SHORT) fields are")
    print("by default cast to FLOAT type in BigQuery. You can choose to cast them to INTEGER instead.")
    print("This affects both auto table creation and schema updates.")

    int_casting_choice = input("Do you want INT8 and INT16 fields to be cast to INTEGER instead of FLOAT? (yes/no, default is no): ").strip().lower()
    if int_casting_choice in ['yes', 'y']:
        use_integer_for_int8_int16 = "true"
        print("INT8 and INT16 fields will be cast to INTEGER type.")
    else:
        use_integer_for_int8_int16 = "false"
        print("INT8 and INT16 fields will be cast to FLOAT type (default behavior).")

    # Get commit interval (only for BATCH LOADING)
    commit_interval = "60"  # Default from template
    if ingestion_mode == "BATCH LOADING":
        print("\n" + "="*60)
        print("⏱️  Commit Interval Configuration")
        print("="*60)
        print("For BATCH LOADING mode, you need to set a commit interval.")
        print("This is the interval (in seconds) when the connector attempts to commit streamed records.")
        print("⚠️  IMPORTANT: On every commit interval, a task calls the CreateWriteStream API")
        print("   which is subject to quota limits. Be careful with frequent commits.")
        print()
        print("Valid range: 60 seconds (1 minute) to 14,400 seconds (4 hours)")
        print()

        while True:
            try:
                commit_interval_input = input("Enter commit interval in seconds (default is 60): ").strip()

                if not commit_interval_input:
                    commit_interval = "60"
                    print("Using default commit interval: 60 seconds (1 minute)")
                    break

                interval = int(commit_interval_input)

                if 60 <= interval <= 14400:
                    commit_interval = str(interval)
                    print(f"✅ Commit interval set to: {commit_interval} seconds")
                    break
                else:
                    print(f"❌ Invalid value: {interval}")
                    print("   Commit interval must be between 60 and 14,400 seconds")
                    print("   Please try again.")

            except ValueError:
                print("❌ Invalid input. Please enter a valid number.")
                print("   Example: 60 for 1 minute, 300 for 5 minutes")

    # Get auto-create tables preference with numbered options
    print("\n🏗️  Auto Create Tables Configuration:")
    print("1. NON-PARTITIONED - Creates tables without partitioning")
    print("2. PARTITION by INGESTION TIME - Creates tables partitioned by ingestion time")
    print("3. PARTITION by FIELD - Creates tables partitioned by a specific timestamp field")
    print("4. DISABLED - Disable auto table creation (tables must exist beforehand)")

    auto_create_choice = input("Choose auto create tables option (1-4, default is 1): ").strip()
    if auto_create_choice == "2":
        auto_create_tables = "PARTITION by INGESTION TIME"
        print("✅ Auto create tables set to: PARTITION by INGESTION TIME")
    elif auto_create_choice == "3":
        auto_create_tables = "PARTITION by FIELD"
        print("✅ Auto create tables set to: PARTITION by FIELD")
    elif auto_create_choice == "4":
        auto_create_tables = "DISABLED"
        print("✅ Auto create tables set to: DISABLED")
    else:
        auto_create_tables = "NON-PARTITIONED"
        print("✅ Auto create tables set to: NON-PARTITIONED (default)")

    # Get partitioning options if auto-create tables is enabled
    partitioning_type = "DAY"  # Default from template
    timestamp_partition_field_name = ""

    if auto_create_tables in ["PARTITION by INGESTION TIME", "PARTITION by FIELD"]:
        print("\n" + "="*50)
        print("⏰ Partitioning Type Configuration")
        print("="*50)
        print("Choose the time partitioning type for your tables:")
        print()
        print("1. HOUR - Partition by hour")
        print("2. DAY - Partition by day")
        print("3. MONTH - Partition by month")
        print("4. YEAR - Partition by year")
        print()

        while True:
            partition_choice = input("Choose partitioning type (1-4, default is 2): ").strip()
            if partition_choice == "1":
                partitioning_type = "HOUR"
                print("✅ Partitioning type set to: HOUR")
                break
            elif partition_choice == "3":
                partitioning_type = "MONTH"
                print("✅ Partitioning type set to: MONTH")
                break
            elif partition_choice == "4":
                partitioning_type = "YEAR"
                print("✅ Partitioning type set to: YEAR")
                break
            else:
                partitioning_type = "DAY"
                print("✅ Partitioning type set to: DAY (default)")
                break

        if auto_create_tables == "PARTITION by FIELD":
            print("\n" + "="*50)
            print("📅 Timestamp Partition Field Configuration")
            print("="*50)
            print("You selected 'PARTITION by FIELD' which requires specifying a timestamp field.")
            print("This field should contain the timestamp value used for partitioning.")
            print("Example field names: 'timestamp', 'created_at', 'event_time', etc.")
            print()

            while True:
                timestamp_field = input("Enter the timestamp field name for partitioning: ").strip()
                if timestamp_field:
                    timestamp_partition_field_name = timestamp_field
                    print(f"✅ Timestamp partition field set to: {timestamp_field}")
                    break
                else:
                    print("❌ Field name cannot be empty. Please try again.")

    # Get date time formatter preference
    print("\n" + "="*50)
    print("📅 Date Time Formatter Configuration")
    print("="*50)
    print("The 'use.date.time.formatter' setting controls how timestamp values are processed:")
    print()
    print("• FALSE (default) - Uses SimpleDateFormat for timestamp parsing")
    print("• TRUE - Uses DateTimeFormatter for better timestamp support")
    print()
    print("DateTimeFormatter supports a wider range of timestamp formats and epochs.")
    print("Note: The output might vary between the two formatters for the same input.")
    print()

    date_formatter_choice = input("Do you want to use DateTimeFormatter? (yes/no, default is no): ").strip().lower()
    if date_formatter_choice in ['yes', 'y']:
        use_date_time_formatter = "true"
        print("✅ DateTimeFormatter will be used for timestamp processing.")
    else:
        use_date_time_formatter = "false"
        print("✅ SimpleDateFormat will be used for timestamp processing (default).")

    return {
        'new_connector_name': new_connector_name,
        'ingestion_mode': ingestion_mode,
        'use_integer_for_int8_int16': use_integer_for_int8_int16,
        'commit_interval': commit_interval,
        'auto_create_tables': auto_create_tables,
        'partitioning_type': partitioning_type,
        'timestamp_partition_field_name': timestamp_partition_field_name,
        'use_date_time_formatter': use_date_time_formatter
    }

def apply_defaults(new_config, user_inputs):
    """Apply default values for missing configurations."""
    print("\n🔧 Applying default values...")

    # Apply only the true defaults from Storage Write API connector template
    # These are configs that have default_value in config_defs and are not handled by user input
    defaults = {
        'input.key.format': 'BYTES',  # Default from template
        'sanitize.topics': 'true',    # Default from template
        'sanitize.field.names': 'false',  # Default from template
        'auto.update.schemas': 'DISABLED',  # Default from template
        'topic2table.map': '',  # Default from template
        'topic2clustering.fields.map': '',  # Default from template
    }

    # Apply defaults only if not already set
    for key, value in defaults.items():
        if key not in new_config:
            new_config[key] = value

    # Set sanitize.field.names.in.array based on sanitize.field.names
    if 'sanitize.field.names' in new_config:
        sanitize_field_names = new_config['sanitize.field.names'].lower() == 'true'
        new_config['sanitize.field.names.in.array'] = str(sanitize_field_names).lower()

    return new_config

def transform_legacy_to_storage(legacy_config):
    """
    Transform a BigQuery Legacy configuration to Storage Write API configuration.
    """
    try:
        # Initialize the Storage Write API configuration
        storage_config = {
            "connector.class": "BigQueryStorageSink",
            "tasks.max": legacy_config.get("tasks.max", 1),
        }

        # Legacy connector only supports service account authentication via keyfile
        if "keyfile" in legacy_config:
            storage_config["authentication.method"] = "Google cloud service account"
        else:
            storage_config["authentication.method"] = "Google cloud service account"

        # Map legacy configurations to Storage Write API configurations
        config_mapping = {
            "topics": "topics",
            "project": "project",
            "defaultDataset": "defaultDataset",
            "keyfile": "keyfile",
            "input.data.format": "input.data.format",
            "sanitize.topics": "sanitize.topics",
            "sanitize.field.names": "sanitize.field.names",
            "auto.update.schemas": "auto.update.schemas",
            "topic2table.map": "topic2table.map",
            "sanitize.field.names.in.array": "sanitize.field.names.in.array"
        }

        # Copy mapped configurations
        for legacy_key, storage_key in config_mapping.items():
            if legacy_key in legacy_config:
                storage_config[storage_key] = legacy_config[legacy_key]

        # Copy common configurations
        for config_key in common_configs:
            if config_key in legacy_config:
                storage_config[config_key] = legacy_config[config_key]

        # Copy additional configurations (excluding unsupported ones)
        for config_key, config_value in legacy_config.items():
            if (config_key not in config_mapping and
                config_key not in common_configs and
                config_key not in UNSUPPORTED_CONFIGS and
                config_key not in ["name", "connector.class", "tasks.max", "authentication.method"]):
                storage_config[config_key] = config_value

        # Apply storage defaults for missing configurations
        for config_key, default_value in storage_defaults.items():
            if config_key not in storage_config:
                storage_config[config_key] = default_value

        return storage_config

    except Exception as e:
        raise Exception(f"Error transforming Legacy to Storage Write API configuration: {e}") from e

def get_keyfile_input():
    """Handle direct keyfile input with better support for large JSON content."""
    print("\n📝 Direct Keyfile Input")
    print("Paste your GCP service account JSON content below.")
    print("Press Enter twice when finished:")

    lines = []
    while True:
        try:
            line = input()
            if line.strip() == "" and lines:  # Empty line after content
                break
            lines.append(line)
        except EOFError:
            break

    keyfile_content = "\n".join(lines)

    # Validate that it's valid JSON
    try:
        json.loads(keyfile_content)
        print("✅ Valid JSON keyfile content received")
        return keyfile_content
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON format: {e}")
        retry = input("Try again? (yes/no): ").strip().lower()
        if retry in ['yes', 'y']:
            return get_keyfile_input()
        else:
            raise Exception("Invalid keyfile JSON format")

def get_auth_token(base_url):
    url = base_url + "api/sessions"
    email = os.environ.get("EMAIL")
    password = os.environ.get("PASSWORD")

    if not email or not password:
        raise APIError("Email or password not found in environment variables")

    json_data = {
        'email': email,
        'password': password
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
        return token
    except json.JSONDecodeError:
        raise APIError("Failed to decode JSON while getting auth token", response_text=response.text)

def get_connector_config(base_url, env, lkc, connector_name):
    global auth_token, last_poll_time
    if (datetime.now() - last_poll_time).total_seconds() > 180:
        auth_token = get_auth_token(base_url)

    cookies = {'auth_token': auth_token}
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
    global auth_token
    if (datetime.now() - last_poll_time).total_seconds() > 180:
        auth_token = get_auth_token(base_url)

    headers = {'Authorization': f'Bearer {auth_token}'}
    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}/offsets"
    response = requests.get(url, headers=headers)

    if not response.ok:
        raise APIError(f"Failed to get connector offsets for {connector_name}: {response.status_code} {response.reason}",
                       status_code=response.status_code,
                       response_text=response.text)

    try:
        json_response = response.json()
        return json_response["offsets"]
    except json.JSONDecodeError:
        raise APIError(f"Failed to decode JSON for connector offsets: {connector_name}", response_text=response.text)

def send_create_request(base_url, env, lkc, connector_name, configs, offsets):
    global auth_token, last_poll_time
    if (datetime.now() - last_poll_time).total_seconds() > 180:
        auth_token = get_auth_token(base_url)

    cookies = {
        'auth_token': auth_token,
    }

    new_connector_name = configs.get("name")

    json_data = {
        'name': new_connector_name,
        'config': configs,
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

def get_connector_status(base_url, env, lkc, connector_name):
    global auth_token, last_poll_time
    if (datetime.now() - last_poll_time).total_seconds() > 180:
        auth_token = get_auth_token(base_url)
        last_poll_time = datetime.now()

    cookies = {'auth_token': auth_token}
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

def check_unsupported_configs(legacy_config):
    """Check for configurations that are not supported in V2 connector."""
    found_unsupported = []

    for config_key in UNSUPPORTED_CONFIGS.keys():
        if config_key in legacy_config:
            found_unsupported.append(config_key)

    return found_unsupported

def show_unsupported_configs_warning(unsupported_configs):
    """Display warning about unsupported configurations."""
    if not unsupported_configs:
        return True

    print("\n" + "="*80)
    print("⚠️  UNSUPPORTED CONFIGURATIONS DETECTED")
    print("="*80)
    print("The following configurations are NOT SUPPORTED in V2 connector:")
    print()

    for config_key in unsupported_configs:
        print(f"• {config_key}: {UNSUPPORTED_CONFIGS[config_key]}")

    print("\n" + "-"*80)
    print("IMPACT: These configurations will be ignored during migration.")
    print("-"*80)

    user_input = input("\nDo you understand that these configurations will not be migrated? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled.")
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Migrate BigQuery Legacy sink connector to Storage Write API.")
    parser.add_argument('--legacy_connector', required=True, help='Name of the Legacy connector')
    parser.add_argument('--environment', required=True, help='Environment ID')
    parser.add_argument('--cluster_id', required=True, help='Cluster ID')
    args = parser.parse_args()

    connector_name = args.legacy_connector
    env = args.environment
    lkc = args.cluster_id

    try:
        # Show breaking changes warning first
        if not show_breaking_changes_warning():
            return

        print("Fetching Legacy connector's status...")
        status = get_connector_status(base_url, env, lkc, connector_name)
        print(f"Connector status for {connector_name}: {status}")
        if status != "PAUSED":
            user_input = input("The connector is not paused. There might be data duplication if you continue. Proceed? (yes/no): ")
            if user_input.lower() != 'yes':
                print("Migration cancelled.")
                return

        print("Fetching legacy connector offsets...")
        offsets = get_connector_offsets(base_url, env, lkc, connector_name)

        print("Fetching Legacy connector's config...")
        legacy_config = get_connector_config(base_url, env, lkc, connector_name)

        # Check for unsupported configurations
        print("Checking for unsupported configurations...")
        unsupported_configs = check_unsupported_configs(legacy_config)
        if not show_unsupported_configs_warning(unsupported_configs):
            return

        # Get user inputs for new connector configuration
        user_inputs = get_user_inputs(legacy_config)

        print("Transforming Legacy connector's config to Storage Write API...")
        storage_config = transform_legacy_to_storage(legacy_config)

        # Update connector name with user input
        storage_config['name'] = user_inputs['new_connector_name']

        # Apply user inputs to storage configuration
        storage_config['ingestion.mode'] = user_inputs['ingestion_mode']
        storage_config['use.integer.for.int8.int16'] = user_inputs['use_integer_for_int8_int16']
        storage_config['use.date.time.formatter'] = user_inputs['use_date_time_formatter']

        # Apply commit interval for BATCH LOADING mode
        if user_inputs['ingestion_mode'] == 'BATCH LOADING':
            storage_config['commit.interval'] = user_inputs['commit_interval']

        # Apply auto-create tables and related configs
        if user_inputs['auto_create_tables'] != 'DISABLED':
            storage_config['auto.create.tables'] = user_inputs['auto_create_tables']
            storage_config['partitioning.type'] = user_inputs['partitioning_type']
            if user_inputs['timestamp_partition_field_name']:
                storage_config['timestamp.partition.field.name'] = user_inputs['timestamp_partition_field_name']

        # Apply default values from Storage Write API connector template
        storage_config = apply_defaults(storage_config, user_inputs)

        # Handle keyfile input specially for large JSON content
        if "keyfile" in storage_config and storage_config["keyfile"] == SCRAPPED_PASSWORD_STRING:
            print("\n" + "="*60)
            print("🔑 GCP Service Account Keyfile Input")
            print("="*60)
            print("Choose how you want to provide the keyfile:")
            print("1. File path - Provide path to JSON file")
            print("2. Environment variable - Set GCP_KEYFILE_PATH environment variable")
            print("3. Direct input - Paste the JSON content directly")

            keyfile_choice = input("Choose option (1-3, default is 1): ").strip()

            if keyfile_choice == "2":
                # Option 2: Environment variable
                keyfile_path = os.environ.get("GCP_KEYFILE_PATH")
                if keyfile_path and os.path.exists(keyfile_path):
                    try:
                        with open(keyfile_path, 'r') as f:
                            storage_config["keyfile"] = f.read()
                        print(f"✅ Keyfile loaded from environment variable: {keyfile_path}")
                    except Exception as e:
                        print(f"❌ Error reading keyfile from {keyfile_path}: {e}")
                        storage_config["keyfile"] = get_keyfile_input()
                else:
                    print("❌ GCP_KEYFILE_PATH environment variable not set or file not found")
                    storage_config["keyfile"] = get_keyfile_input()
            elif keyfile_choice == "3":
                # Option 3: Direct input
                storage_config["keyfile"] = get_keyfile_input()
            else:
                # Option 1: File path (default)
                while True:
                    keyfile_path = input("Enter the path to your GCP service account JSON file: ").strip()
                    if keyfile_path and os.path.exists(keyfile_path):
                        try:
                            with open(keyfile_path, 'r') as f:
                                storage_config["keyfile"] = f.read()
                            print(f"✅ Keyfile loaded successfully from: {keyfile_path}")
                            break
                        except Exception as e:
                            print(f"❌ Error reading file: {e}")
                            retry = input("Try again? (yes/no): ").strip().lower()
                            if retry not in ['yes', 'y']:
                                storage_config["keyfile"] = get_keyfile_input()
                                break
                    else:
                        print("❌ File not found. Please provide a valid file path.")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            storage_config["keyfile"] = get_keyfile_input()
                            break

        # Prompt user to input values for other fields with "****************"
        for key, value in storage_config.items():
            if value == SCRAPPED_PASSWORD_STRING and key != "keyfile":  # Skip keyfile as it's handled above
                while True:
                    user_input = input(f"Please enter the value for {key}: ").strip()
                    if user_input:
                        storage_config[key] = user_input
                        break
                    else:
                        print("Input cannot be empty. Please try again.")

        # Display the Storage Write API configuration and ask for confirmation
        print("\n" + "="*80)
        print("📋 FINAL STORAGE WRITE API CONNECTOR CONFIGURATION")
        print("="*80)
        print(json.dumps(storage_config, indent=4))
        print("="*80)

        user_input = input("\nPlease review the above configuration. Do you want to proceed with creating the Storage Write API connector? (yes/no): ")
        if user_input.lower() != 'yes':
            print("Migration cancelled.")
            return

        print("Creating Storage Write API connector...")
        send_create_request(base_url, env, lkc, user_inputs['new_connector_name'], storage_config, offsets)

        print("\n" + "="*80)
        print("✅ MIGRATION COMPLETED SUCCESSFULLY")
        print("="*80)
        print("Next steps:")
        print("1. Verify the new connector is running properly")
        print("2. Check data integrity in BigQuery")
        print("3. Monitor for any data type related issues")
        print("="*80)

    except APIError as e:
        print(f"Encountered Error: {e}, Status Code: {e.status_code}, Response: {e.response_text}")
    except Exception as e:
        print(f"An error occurred while running the migration tool: {e}")

if __name__ == '__main__':
    base_url = "https://confluent.cloud/"
    auth_token = get_auth_token(base_url)
    last_poll_time = datetime.now()
    main()
