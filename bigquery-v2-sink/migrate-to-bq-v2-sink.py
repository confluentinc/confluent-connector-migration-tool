#!/usr/bin/env python3
"""
BigQuery Legacy to Storage Write API Migration Tool

This tool migrates BigQuery Legacy (InsertAll API) connectors to BigQuery Storage Write API
connectors in Confluent Cloud. The Storage Write API offers better performance and cost
efficiency compared to the Legacy InsertAll API.
"""

import argparse
import os
import json
import sys

# Add parent directory to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.migration_utils import (
    BASE_URL,
    SCRUBBED_PASSWORD_STRING,
    APIError,
    get_connector_config,
    get_connector_offsets,
    get_connector_status,
    send_create_request,
    prompt_for_sensitive_values,
    display_config_and_confirm,
    initialize_auth,
    check_connector_status_and_confirm
)


# ============================================================================
# Configuration Mappings
# ============================================================================

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
    "allow.schema.unionization": "Schema unionization is not supported in V2 connector. This functionality is now part of the auto.update.schemas property, which handles schema evolution for both primitive and complex types (structs and arrays).",
    "all.bq.fields.nullable": "All BigQuery fields nullable setting is not supported in V2 connector. This controlled whether all fields were made nullable.",
    "convert.double.special.values": "Double special values conversion is not supported in V2 connector. This handled +Infinity, -Infinity, and NaN conversions.",
    "allow.bigquery.required.field.relaxation": "BigQuery required field relaxation is not supported in V2 connector. This allowed relaxing required field constraints."
}


# ============================================================================
# Helper Functions
# ============================================================================

def show_breaking_changes_warning():
    """Display breaking changes warning to the user."""
    print("\n" + "="*80)
    print("IMPORTANT: BREAKING API CHANGES")
    print("="*80)
    print("The BigQuery Storage Write API has breaking changes from the Legacy InsertAll API:")
    print()

    for change_type, description in BREAKING_CHANGES.items():
        print(f"  * {change_type}: {description}")

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
    print("UNSUPPORTED CONFIGURATIONS DETECTED")
    print("="*80)
    print("The following configurations are NOT SUPPORTED in V2 connector:")
    print()

    # Check if any schema-related configs are present
    schema_unionization_config = "allow.schema.unionization"
    required_field_relaxation_config = "allow.bigquery.required.field.relaxation"
    has_schema_unionization = schema_unionization_config in unsupported_configs
    has_required_field_relaxation = required_field_relaxation_config in unsupported_configs

    for config_key in unsupported_configs:
        print(f"  * {config_key}: {UNSUPPORTED_CONFIGS[config_key]}")

    print("\n" + "-"*80)
    print("IMPACT: These configurations will be ignored during migration.")

    if has_schema_unionization:
        print("\nSCHEMA EVOLUTION IN V2:")
        print("The V2 connector handles schema evolution through the 'auto.update.schemas' property:")
        print("  * 'DISABLED' - No automatic schema updates")
        print("  * 'ADD NEW FIELDS' - Automatically adds new fields to existing tables")
        print("This covers both primitive and complex types (structs and arrays).")
        print("The migration script will set this based on your legacy 'auto.update.schemas' setting.")

    if has_required_field_relaxation:
        print("\nFIELD NULLABILITY IN V2:")
        print("All fields created through V2 connector are nullable by default.")
        print("The 'allow.bigquery.required.field.relaxation' configuration is no longer supported in V2 connector.")

    print("-"*80)

    user_input = input("\nDo you understand that these configurations will not be migrated? (yes/no): ")
    if user_input.lower() != 'yes':
        print("Migration cancelled.")
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
            print("New connector name must be different from the legacy connector name.")

    # Get ingestion mode with numbered options
    print("\nIngestion Mode Selection:")
    print("  1. STREAMING - Lower latency, higher cost")
    print("  2. BATCH LOADING - Higher latency, lower cost")
    print("  3. UPSERT - For upsert operations")
    print("  4. UPSERT_DELETE - For upsert and delete operations")

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
    print("\nInt8/Int16 Type Casting:")
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
        print("Commit Interval Configuration")
        print("="*60)
        print("For BATCH LOADING mode, you need to set a commit interval.")
        print("This is the interval (in seconds) when the connector attempts to commit streamed records.")
        print("IMPORTANT: On every commit interval, a task calls the CreateWriteStream API")
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
                    print(f"Commit interval set to: {commit_interval} seconds")
                    break
                else:
                    print(f"Invalid value: {interval}")
                    print("   Commit interval must be between 60 and 14,400 seconds")
                    print("   Please try again.")

            except ValueError:
                print("Invalid input. Please enter a valid number.")
                print("   Example: 60 for 1 minute, 300 for 5 minutes")

    # Get auto-create tables preference with numbered options (changed default to DISABLED)
    print("\nAuto Create Tables Configuration:")
    print("  1. DISABLED - Disable auto table creation (tables must exist beforehand)")
    print("  2. NON-PARTITIONED - Creates tables without partitioning")
    print("  3. PARTITION by INGESTION TIME - Creates tables partitioned by ingestion time")
    print("  4. PARTITION by FIELD - Creates tables partitioned by a specific timestamp field")

    auto_create_choice = input("Choose auto create tables option (1-4, default is 1 for DISABLED): ").strip()
    if auto_create_choice == "2":
        auto_create_tables = "NON-PARTITIONED"
        print("Auto create tables set to: NON-PARTITIONED")
    elif auto_create_choice == "3":
        auto_create_tables = "PARTITION by INGESTION TIME"
        print("Auto create tables set to: PARTITION by INGESTION TIME")
    elif auto_create_choice == "4":
        auto_create_tables = "PARTITION by FIELD"
        print("Auto create tables set to: PARTITION by FIELD")
    else:
        auto_create_tables = "DISABLED"
        print("Auto create tables set to: DISABLED (default)")

    # Get partitioning options if auto-create tables is enabled
    partitioning_type = "DAY"  # Default from template
    timestamp_partition_field_name = ""

    if auto_create_tables in ["PARTITION by INGESTION TIME", "PARTITION by FIELD"]:
        print("\n" + "="*50)
        print("Partitioning Type Configuration")
        print("="*50)
        print("Choose the time partitioning type for your tables:")
        print()
        print("  1. HOUR - Partition by hour")
        print("  2. DAY - Partition by day")
        print("  3. MONTH - Partition by month")
        print("  4. YEAR - Partition by year")
        print()

        while True:
            partition_choice = input("Choose partitioning type (1-4, default is 2): ").strip()
            if partition_choice == "1":
                partitioning_type = "HOUR"
                print("Partitioning type set to: HOUR")
                break
            elif partition_choice == "3":
                partitioning_type = "MONTH"
                print("Partitioning type set to: MONTH")
                break
            elif partition_choice == "4":
                partitioning_type = "YEAR"
                print("Partitioning type set to: YEAR")
                break
            else:
                partitioning_type = "DAY"
                print("Partitioning type set to: DAY (default)")
                break

        if auto_create_tables == "PARTITION by FIELD":
            print("\n" + "="*50)
            print("Timestamp Partition Field Configuration")
            print("="*50)
            print("You selected 'PARTITION by FIELD' which requires specifying a timestamp field.")
            print("This field should contain the timestamp value used for partitioning.")
            print("Example field names: 'timestamp', 'created_at', 'event_time', etc.")
            print()

            while True:
                timestamp_field = input("Enter the timestamp field name for partitioning: ").strip()
                if timestamp_field:
                    timestamp_partition_field_name = timestamp_field
                    print(f"Timestamp partition field set to: {timestamp_field}")
                    break
                else:
                    print("Field name cannot be empty. Please try again.")

    # Get testing configuration for project, dataset, and topic2table mapping
    print("\n" + "="*60)
    print("Testing Configuration")
    print("="*60)
    print("For testing purposes, you can configure project, dataset, and topic2table mapping")
    print("to write to different BigQuery resources. This allows you to test the migration")
    print("without affecting your production BigQuery tables.")
    print()

    # Show current configurations
    current_project = legacy_config.get("project", "")
    current_dataset = legacy_config.get("datasets", "")  # V1 uses "datasets", V2 uses "defaultDataset"
    existing_topic2table_map = legacy_config.get("topic2table.map", "")

    print("Current configurations:")
    print(f"  * Project: {current_project if current_project else '(not configured)'}")
    print(f"  * Dataset: {current_dataset if current_dataset else '(not configured)'}")
    print(f"  * Topic2Table Map: {existing_topic2table_map if existing_topic2table_map else '(not configured)'}")
    print()

    testing_choice = input("Do you want to update project, dataset, or topic2table mapping for testing? (yes/no, default is no): ").strip().lower()

    # Initialize with current values
    project_for_migration = current_project
    dataset_for_migration = current_dataset
    topic2table_map = existing_topic2table_map

    if testing_choice in ['yes', 'y']:
        print("\n" + "="*50)
        print("Testing Configuration Setup")
        print("="*50)

        # Project configuration
        print(f"\nCurrent project: {current_project if current_project else '(not configured)'}")
        project_update = input("Do you want to update the project for testing? (yes/no): ").strip().lower()

        if project_update in ['yes', 'y']:
            while True:
                new_project = input("Enter new project ID for testing: ").strip()
                if new_project:
                    project_for_migration = new_project
                    print(f"Project set to: {new_project}")
                    break
                else:
                    print("Project ID cannot be empty. Please try again.")
        else:
            print(f"Using existing project: {current_project}")

        # Dataset configuration
        print(f"\nCurrent dataset: {current_dataset if current_dataset else '(not configured)'}")
        dataset_update = input("Do you want to update the dataset for testing? (yes/no): ").strip().lower()

        if dataset_update in ['yes', 'y']:
            while True:
                new_dataset = input("Enter new dataset name for testing: ").strip()
                if new_dataset:
                    dataset_for_migration = new_dataset
                    print(f"Dataset set to: {new_dataset}")
                    break
                else:
                    print("Dataset name cannot be empty. Please try again.")
        else:
            print(f"Using existing dataset: {current_dataset}")

        # Topic2Table mapping configuration
        print(f"\nCurrent topic2table mapping: {existing_topic2table_map if existing_topic2table_map else '(not configured)'}")
        topic2table_update = input("Do you want to update the topic2table mapping for testing? (yes/no): ").strip().lower()

        if topic2table_update in ['yes', 'y']:
            print("\nTopic to Table Mapping Input")
            print("Enter the mapping in format: topic1:table1,topic2:table2")
            print("Example: my-topic:my-test-table,another-topic:another-test-table")
            print("This will redirect data to test tables instead of production tables.")

            while True:
                new_topic2table_map = input("Enter topic2table mapping: ").strip()
                if new_topic2table_map:
                    topic2table_map = new_topic2table_map
                    print(f"Topic to table mapping set to: {new_topic2table_map}")
                    break
                else:
                    print("Mapping cannot be empty. Please try again.")
        else:
            print(f"Using existing topic2table mapping: {existing_topic2table_map}")

        print("\n" + "="*50)
        print("Testing Configuration Summary")
        print("="*50)
        print(f"  * Project: {project_for_migration}")
        print(f"  * Dataset: {dataset_for_migration}")
        print(f"  * Topic2Table Map: {topic2table_map}")
        print("="*50)
    else:
        print("Using existing configurations for all settings")

    # Check if auto-create tables is disabled and provide table creation guidance
    if auto_create_tables == "DISABLED":
        print("\n" + "="*60)
        print("Table Creation Guidance")
        print("="*60)
        print("Auto-create tables is set to DISABLED. You may need to create tables manually.")
        print()
        print("If you need to create tables with the same schema as existing tables, use:")
        print("CREATE TABLE `project-id.dataset-name.new_table_name`")
        print("LIKE `project-id.dataset-name.source_table_name`;")
        print()
        print("Replace with your actual project ID, dataset name, and table names.")
        print("="*60)

    # Get date time formatter preference
    print("\n" + "="*50)
    print("Date Time Formatter Configuration")
    print("="*50)
    print("The 'use.date.time.formatter' setting controls how timestamp values are processed:")
    print()
    print("  * FALSE (default) - Uses SimpleDateFormat for timestamp parsing")
    print("  * TRUE - Uses DateTimeFormatter for better timestamp support")
    print()
    print("DateTimeFormatter supports a wider range of timestamp formats and epochs.")
    print("Note: The output might vary between the two formatters for the same input.")
    print()

    date_formatter_choice = input("Do you want to use DateTimeFormatter? (yes/no, default is no): ").strip().lower()
    if date_formatter_choice in ['yes', 'y']:
        use_date_time_formatter = "true"
        print("DateTimeFormatter will be used for timestamp processing.")
    else:
        use_date_time_formatter = "false"
        print("SimpleDateFormat will be used for timestamp processing (default).")

    return {
        'new_connector_name': new_connector_name,
        'ingestion_mode': ingestion_mode,
        'use_integer_for_int8_int16': use_integer_for_int8_int16,
        'commit_interval': commit_interval,
        'auto_create_tables': auto_create_tables,
        'partitioning_type': partitioning_type,
        'timestamp_partition_field_name': timestamp_partition_field_name,
        'topic2table_map': topic2table_map,
        'project_for_migration': project_for_migration,
        'dataset_for_migration': dataset_for_migration,
        'use_date_time_formatter': use_date_time_formatter
    }


def apply_defaults(new_config, user_inputs):
    """Apply default values for missing configurations."""
    print("\nApplying default values...")

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
            "datasets": "datasets",
            "keyfile": "keyfile",
            "input.data.format": "input.data.format",
            "sanitize.topics": "sanitize.topics",
            "sanitize.field.names": "sanitize.field.names",
            "topic2table.map": "topic2table.map",
            "sanitize.field.names.in.array": "sanitize.field.names.in.array"
        }

        # Copy mapped configurations
        for legacy_key, storage_key in config_mapping.items():
            if legacy_key in legacy_config:
                storage_config[storage_key] = legacy_config[legacy_key]

        # Handle auto.update.schemas transformation from v1 to v2 format
        if "auto.update.schemas" in legacy_config:
            v1_value = legacy_config["auto.update.schemas"]
            if v1_value.lower() == "true":
                storage_config["auto.update.schemas"] = "ADD NEW FIELDS"
                print(f"Transformed auto.update.schemas: '{v1_value}' -> 'ADD NEW FIELDS'")
            elif v1_value.lower() == "false":
                storage_config["auto.update.schemas"] = "DISABLED"
                print(f"Transformed auto.update.schemas: '{v1_value}' -> 'DISABLED'")
            else:
                # Handle unexpected values - default to DISABLED for safety
                print(f"Warning: Unexpected auto.update.schemas value '{v1_value}' in legacy config. Defaulting to 'DISABLED'.")
                storage_config["auto.update.schemas"] = "DISABLED"

        # Copy common configurations
        for config_key in common_configs:
            if config_key in legacy_config:
                storage_config[config_key] = legacy_config[config_key]

        # Copy additional configurations (excluding unsupported ones)
        for config_key, config_value in legacy_config.items():
            if (config_key not in config_mapping and
                config_key not in common_configs and
                config_key not in UNSUPPORTED_CONFIGS and
                config_key not in ["name", "connector.class", "tasks.max", "authentication.method", "auto.update.schemas"]):
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
    print("\nDirect Keyfile Input")
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
        print("Valid JSON keyfile content received")
        return keyfile_content
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        retry = input("Try again? (yes/no): ").strip().lower()
        if retry in ['yes', 'y']:
            return get_keyfile_input()
        else:
            raise Exception("Invalid keyfile JSON format")


def handle_keyfile_input(storage_config):
    """Handle keyfile input for BigQuery authentication."""
    if "keyfile" in storage_config and storage_config["keyfile"] == SCRUBBED_PASSWORD_STRING:
        print("\n" + "="*60)
        print("GCP Service Account Keyfile Input")
        print("="*60)
        print("Choose how you want to provide the keyfile:")
        print("  1. File path - Provide path to JSON file")
        print("  2. Environment variable - Set GCP_KEYFILE_PATH environment variable")
        print("  3. Direct input - Paste the JSON content directly")

        keyfile_choice = input("Choose option (1-3, default is 1): ").strip()

        if keyfile_choice == "2":
            # Option 2: Environment variable
            keyfile_path = os.environ.get("GCP_KEYFILE_PATH")
            if keyfile_path and os.path.exists(keyfile_path):
                try:
                    with open(keyfile_path, 'r') as f:
                        storage_config["keyfile"] = f.read()
                    print(f"Keyfile loaded from environment variable: {keyfile_path}")
                except Exception as e:
                    print(f"Error reading keyfile from {keyfile_path}: {e}")
                    storage_config["keyfile"] = get_keyfile_input()
            else:
                print("GCP_KEYFILE_PATH environment variable not set or file not found")
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
                        print(f"Keyfile loaded successfully from: {keyfile_path}")
                        break
                    except Exception as e:
                        print(f"Error reading file: {e}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            storage_config["keyfile"] = get_keyfile_input()
                            break
                else:
                    print("File not found. Please provide a valid file path.")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        storage_config["keyfile"] = get_keyfile_input()
                        break

    return storage_config


# ============================================================================
# Main Migration Flow
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Migrate BigQuery V1 Legacy sink connector to BigQuery V2 Storage Write API Connector."
    )
    parser.add_argument('--legacy_connector', required=True, help='Name of the Legacy connector')
    parser.add_argument('--environment', required=True, help='Environment ID')
    parser.add_argument('--cluster_id', required=True, help='Cluster ID')
    args = parser.parse_args()

    connector_name = args.legacy_connector
    env = args.environment
    lkc = args.cluster_id

    try:
        # Step 1: Show breaking changes warning
        print("\n" + "="*80)
        print("BIGQUERY LEGACY TO STORAGE WRITE API MIGRATION TOOL")
        print("="*80)
        print(f"Migrating connector: {connector_name}")
        print(f"Environment: {env}")
        print(f"Cluster: {lkc}")

        if not show_breaking_changes_warning():
            return

        # Step 2: Get credentials and authenticate
        initialize_auth(BASE_URL)

        # Step 3: Check legacy connector status
        print("\nFetching Legacy connector status...")
        status = get_connector_status(BASE_URL, env, lkc, connector_name)

        if not check_connector_status_and_confirm(status, connector_name):
            return

        # Step 4: Fetch legacy config and offsets
        print("\nFetching legacy connector offsets...")
        offsets = get_connector_offsets(BASE_URL, env, lkc, connector_name)
        print(f"Retrieved {len(offsets)} offset entries")

        print("Fetching Legacy connector configuration...")
        legacy_config = get_connector_config(BASE_URL, env, lkc, connector_name)
        print(f"Retrieved {len(legacy_config)} configuration properties")

        # Step 5: Check for unsupported configs
        print("\nChecking for unsupported configurations...")
        unsupported_configs = check_unsupported_configs(legacy_config)
        if not show_unsupported_configs_warning(unsupported_configs):
            return

        # Step 6: Get user inputs for new connector configuration
        user_inputs = get_user_inputs(legacy_config)

        # Step 7: Transform legacy -> storage config
        print("\nTransforming Legacy configuration to Storage Write API...")
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
        storage_config['auto.create.tables'] = user_inputs['auto_create_tables']
        if user_inputs['auto_create_tables'] != 'DISABLED':
            storage_config['partitioning.type'] = user_inputs['partitioning_type']
            if user_inputs['timestamp_partition_field_name']:
                storage_config['timestamp.partition.field.name'] = user_inputs['timestamp_partition_field_name']

        # Apply topic2table.map configuration
        if user_inputs['topic2table_map']:
            storage_config['topic2table.map'] = user_inputs['topic2table_map']

        # Apply project and dataset configuration
        if user_inputs['project_for_migration']:
            storage_config['project'] = user_inputs['project_for_migration']
        if user_inputs['dataset_for_migration']:
            storage_config['datasets'] = user_inputs['dataset_for_migration']

        # Apply default values from Storage Write API connector template
        storage_config = apply_defaults(storage_config, user_inputs)

        # Step 8: Handle keyfile input
        storage_config = handle_keyfile_input(storage_config)

        # Step 9: Prompt for any other masked sensitive values
        storage_config = prompt_for_sensitive_values(
            storage_config,
            SCRUBBED_PASSWORD_STRING,
            skip_keys=["keyfile"]  # Already handled
        )

        # Step 10: Display final config for confirmation
        mask_keys = ["keyfile", "kafka.api.secret", "schema.registry.basic.auth.user.info"]

        if not display_config_and_confirm(storage_config,
                                           "Do you want to proceed with creating the Storage Write API connector?",
                                           mask_keys=mask_keys):
            print("Migration cancelled.")
            return

        # Step 11: Create Storage Write API connector with preserved offsets
        print("\nCreating Storage Write API connector with preserved offsets...")
        send_create_request(BASE_URL, env, lkc, user_inputs['new_connector_name'], storage_config, offsets)

        # Step 12: Show next steps
        print("\n" + "="*80)
        print("MIGRATION COMPLETED SUCCESSFULLY")
        print("="*80)
        print("\nNext steps:")
        print("  1. Verify the new connector is running properly in Confluent Cloud Console")
        print("  2. Check data integrity in BigQuery")
        print("  3. Monitor for any data type related issues")
        print("  4. Once verified, you can delete the old Legacy connector")
        print()
        print("IMPORTANT: The Storage Write API connector starts from the preserved Legacy offsets,")
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
