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

# Add parent directory to path for utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.migration_utils import (
    BASE_URL,
    SCRUBBED_PASSWORD_STRING,
    APIError,
    MigrationClient,
    prompt_for_sensitive_values,
    display_config_and_confirm,
    check_connector_status_and_confirm
)


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
    print("     * auto.create will be set to 'false'")
    print("     * You must provide topic-to-index mapping")
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

    # Step 2: Get topic-to-index mapping
    print()
    print("Enter topic-to-index mapping.")
    print("Format: topic1:index1,topic2:index2")
    print(f"Topics to map: {', '.join(topics_list)}")
    print()

    while True:
        mapping_input = input("Enter topic-to-index mapping: ").strip()
        if not mapping_input:
            print("Mapping cannot be empty in test mode.")
            continue

        # Parse and validate mapping
        mapping_topics = set()
        valid_format = True
        for pair in mapping_input.split(","):
            pair = pair.strip()
            if ":" not in pair:
                print(f"Invalid format '{pair}'. Expected 'topic:index'")
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
        print("  V2 only supports a single URL. Please enter the URL to use.")

        while True:
            connection_url = input("Enter connection.url: ").strip()
            if not connection_url:
                print("URL cannot be empty. Please try again.")
                continue
            elif "," in connection_url:
                print("V2 only supports a single URL. Please enter a single URL.")
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
                raise ValueError("BASIC auth requires both username AND password!")
            break

        elif auth_choice == "2":
            auth_type = "API_KEY"
            api_key_value = getpass.getpass("  Enter api.key.value: ")

            if not api_key_value:
                print("ERROR: API_KEY auth requires api.key.value!")
                raise ValueError("API_KEY auth requires api.key.value!")
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
    # This includes: direct configs, SSL configs, SMT transforms, kafka.*, schema.registry.*, etc.
    excluded_keys = set(
        list(RENAMED_MAPPING.keys()) + list(DISCONTINUED.keys()) +
        list(SSL_FILE_CONFIGS) +
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
        client = MigrationClient()
        client.initialize_auth()

        # Step 3: Check V1 connector status
        print("\nFetching V1 connector status...")
        status = client.get_connector_status(env, lkc, connector_name)

        if not check_connector_status_and_confirm(status, connector_name):
            return

        # Step 4: Fetch V1 config and offsets
        print("\nFetching V1 connector offsets...")
        offsets = client.get_connector_offsets(env, lkc, connector_name)
        print(f"Retrieved {len(offsets)} offset entries")

        print("Fetching V1 connector configuration...")
        v1_config = client.get_connector_config(env, lkc, connector_name)
        print(f"Retrieved {len(v1_config)} configuration properties")

        # Step 5: Check for discontinued configs
        print("\nChecking for discontinued configurations...")
        discontinued_configs = check_discontinued_configs(v1_config)
        if discontinued_configs:
            if not show_discontinued_configs_warning(discontinued_configs, v1_config):
                return

        # Step 6: Choose migration mode
        migration_mode = ask_migration_mode()

        # Step 6b: Get test mode configuration if applicable
        test_config = None
        if migration_mode == 'test':
            test_config = get_test_mode_configuration(v1_config)

        # Step 7: Derive V2 properties
        print("\nDeriving V2 properties from V1 configuration...")
        derived = derive_v2_properties(v1_config)
        print(f"  - SSL enabled: {derived['ssl_enabled']}")
        print(f"  - Auto create: {derived['auto_create']}")
        print(f"  - Resource type: {derived['resource_type']}")

        # Step 8: Get user inputs for V2 properties
        user_inputs = get_user_inputs(v1_config, derived, migration_mode)

        # Step 9: Transform V1 -> V2 config
        print("\nTransforming V1 configuration to V2...")
        v2_config, warnings = transform_v1_to_v2(v1_config, user_inputs, derived)

        # Step 9b: Apply test mode overrides if applicable
        if test_config:
            v2_config, test_warnings = apply_test_mode_overrides(v2_config, test_config)
            warnings.extend(test_warnings)

        display_transformation_warnings(warnings)

        # Step 10: Prompt for any masked sensitive values
        v2_config = prompt_for_sensitive_values(
            v2_config,
            SCRUBBED_PASSWORD_STRING,
            skip_keys=["connection.password"]  # Already handled
        )

        # Step 11: Display final config for confirmation
        mask_keys = ["connection.password", "api.key.value", "kafka.api.secret",
                     "schema.registry.basic.auth.user.info"]

        if not display_config_and_confirm(v2_config,
                                           "Do you want to proceed with creating the V2 connector?",
                                           mask_keys=mask_keys):
            print("Migration cancelled.")
            return

        # Step 12: Create V2 connector with preserved offsets
        print("\nCreating V2 connector with preserved offsets...")
        client.send_create_request(env, lkc, user_inputs['new_connector_name'], v2_config, offsets)

        # Step 13: Show next steps
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
