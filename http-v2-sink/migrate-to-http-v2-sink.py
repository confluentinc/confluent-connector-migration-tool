#!/usr/bin/env python3
"""
HTTP Sink V1 to V2 Migration Tool

This tool migrates HTTP Sink V1 connectors to V2 connectors in Confluent Cloud.
V2 connector offers improved architecture with multi-API support and better
configuration organization.
"""

import argparse
import json
import sys
import os

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

# Define the mapping between V1 and V2 configurations
v1_to_v2_mapping = {
    "auth.type": "auth.type",
    "batch.json.as.array": "api1.batch.json.as.array",
    "batch.key.pattern": "api1.batch.key.pattern",
    "batch.max.size": "api1.max.batch.size",
    "batch.prefix": "api1.batch.prefix",
    "batch.separator": "api1.batch.separator",
    "batch.suffix": "api1.batch.suffix",
    "behavior.on.error": "behavior.on.error",
    "behavior.on.null.values": "api1.behavior.on.null.values",
    "connection.password": "connection.password",
    "connection.user": "connection.user",
    "header.separator": "api1.http.request.headers.separator",
    "headers": "api1.http.request.headers",
    "http.connect.timeout.ms": "api1.http.connect.timeout.ms",
    "http.request.timeout.ms": "api1.http.request.timeout.ms",
    "https.host.verifier.enabled": "https.host.verifier.enabled",
    "https.ssl.key.password": "https.ssl.key.password",
    "https.ssl.keystore.password": "https.ssl.keystore.password",
    "https.ssl.keystorefile": "https.ssl.keystorefile",
    "https.ssl.protocol": "https.ssl.protocol",
    "https.ssl.truststore.password": "https.ssl.truststore.password",
    "https.ssl.truststorefile": "https.ssl.truststorefile",
    "max.retries": "api1.max.retries",
    "oauth2.client.auth.mode": "oauth2.client.auth.mode",
    "oauth2.client.header.separator": "oauth2.client.header.separator",
    "oauth2.client.headers": "oauth2.client.headers",
    "oauth2.client.id": "oauth2.client.id",
    "oauth2.client.scope": "oauth2.client.scope",
    "oauth2.client.secret": "oauth2.client.secret",
    "oauth2.jwt.claimset": "oauth2.jwt.claimset",
    "oauth2.jwt.enabled": "oauth2.jwt.enabled",
    "oauth2.jwt.keystore.password": "oauth2.jwt.keystore.password",
    "oauth2.jwt.keystore.path": "oauth2.jwt.keystore.path",
    "oauth2.jwt.keystore.type": "oauth2.jwt.keystore.type",
    "oauth2.token.property": "oauth2.token.property",
    "oauth2.token.url": "oauth2.token.url",
    "regex.patterns": "api1.regex.patterns",
    "regex.replacements": "api1.regex.replacements",
    "regex.separator": "api1.regex.separator",
    "report.errors.as": "report.errors.as",
    "request.body.format": "api1.request.body.format",
    "request.method": "api1.http.request.method",
    "retry.backoff.ms": "api1.retry.backoff.ms",
    "retry.backoff.policy": "api1.retry.backoff.policy",
    "retry.on.status.codes": "api1.retry.on.status.codes",
    "sensitive.headers": "api1.http.request.sensitive.headers",
    "topics": "api1.topics"
}

# Common configurations to be copied as is
common_configs = [
    "input.data.format",
    "kafka.api.key",
    "kafka.api.secret",
    "kafka.auth.mode",
    "kafka.service.account.id",
    "max.poll.interval.ms",
    "max.poll.records",
    "name",
    "schema.context.name",
    "tasks.max",
    "topics"
]


# ============================================================================
# Helper Functions
# ============================================================================

def transform_v1_to_v2(v1_config):
    """
    Transform a V1 configuration to a V2 configuration.
    """
    try:
        # Extract base URL and path from http.api.url
        http_api_url = v1_config.get("http.api.url", "")
        if not http_api_url:
            raise Exception("Missing 'http.api.url' in V1 configuration.")

        parts = http_api_url.split("/")
        base_url = "/".join(parts[:3])
        api_path = "/" + "/".join(parts[3:])

        # Initialize the V2 configuration
        v2_config = {
            "connector.class": "HttpSinkV2",
            "tasks.max": v1_config.get("tasks.max", 1),
            "apis.num": "1",
            "api1.http.api.path": api_path,
        }

        # Copy configurations using the mapping
        for v1_key, v2_key in v1_to_v2_mapping.items():
            if v1_key in v1_config:
                v2_config[v2_key] = v1_config[v1_key]

        # Copy common configurations as is
        for common_key in common_configs:
            if common_key in v1_config:
                v2_config[common_key] = v1_config[common_key]

        # Ensure http.api.base.url is set after copying configurations
        v2_config["http.api.base.url"] = base_url

        return v2_config
    except Exception as e:
        raise Exception(f"Error transforming V1 to V2 configuration: {e}") from e


def get_user_inputs(v1_config):
    """Get user inputs for V2 connector configuration."""
    print("\n" + "="*60)
    print("MIGRATION CONFIGURATION")
    print("="*60)

    # Get new connector name
    default_name = f"{v1_config.get('name', 'http-sink')}-v2"
    while True:
        new_name = input(f"\nEnter new connector name (default: {default_name}): ").strip() or default_name
        if new_name != v1_config.get('name'):
            break
        else:
            print("New connector name must be different from the V1 connector name.")

    return {
        'new_connector_name': new_name
    }


# ============================================================================
# Main Migration Flow
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Migrate HTTP V1 sink connector to V2."
    )
    parser.add_argument('--v1_connector', required=True, help='Name of the V1 connector')
    parser.add_argument('--environment', required=True, help='Environment ID')
    parser.add_argument('--cluster_id', required=True, help='Cluster ID')
    args = parser.parse_args()

    connector_name = args.v1_connector
    env = args.environment
    lkc = args.cluster_id

    try:
        # Step 1: Show migration info
        print("\n" + "="*80)
        print("HTTP SINK V1 TO V2 MIGRATION TOOL")
        print("="*80)
        print(f"Migrating connector: {connector_name}")
        print(f"Environment: {env}")
        print(f"Cluster: {lkc}")

        # Step 2: Get credentials and authenticate
        initialize_auth(BASE_URL)

        # Step 3: Check V1 connector status
        print("\nFetching V1 connector status...")
        status = get_connector_status(BASE_URL, env, lkc, connector_name)

        if not check_connector_status_and_confirm(status, connector_name):
            return

        # Step 4: Fetch V1 config and offsets
        print("\nFetching V1 connector offsets...")
        offsets = get_connector_offsets(BASE_URL, env, lkc, connector_name)
        print(f"Retrieved {len(offsets)} offset entries")

        print("Fetching V1 connector configuration...")
        v1_config = get_connector_config(BASE_URL, env, lkc, connector_name)
        print(f"Retrieved {len(v1_config)} configuration properties")

        # Step 5: Get user inputs
        user_inputs = get_user_inputs(v1_config)

        # Step 6: Transform V1 -> V2 config
        print("\nTransforming V1 configuration to V2...")
        v2_config = transform_v1_to_v2(v1_config)

        # Update connector name with user input
        v2_config['name'] = user_inputs['new_connector_name']

        # Step 7: Prompt for any masked sensitive values
        v2_config = prompt_for_sensitive_values(v2_config, SCRUBBED_PASSWORD_STRING)

        # Step 8: Display final config for confirmation
        mask_keys = ["connection.password", "kafka.api.secret", "oauth2.client.secret",
                     "https.ssl.key.password", "https.ssl.keystore.password",
                     "https.ssl.truststore.password", "oauth2.jwt.keystore.password"]

        if not display_config_and_confirm(v2_config,
                                           "Do you want to proceed with creating the V2 connector?",
                                           mask_keys=mask_keys):
            print("Migration cancelled.")
            return

        # Step 9: Create V2 connector with preserved offsets
        print("\nCreating V2 connector with preserved offsets...")
        send_create_request(BASE_URL, env, lkc, user_inputs['new_connector_name'], v2_config, offsets)

        # Step 10: Show next steps
        print("\n" + "="*80)
        print("MIGRATION COMPLETED SUCCESSFULLY")
        print("="*80)
        print("\nNext steps:")
        print("  1. Verify the new V2 connector is running properly in Confluent Cloud Console")
        print("  2. Check that data is being sent to the HTTP endpoint correctly")
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
