import argparse
import os
import json
from datetime import datetime
import requests
import getpass

auth_token = None
last_poll_time = datetime.now()
SCRAPPED_PASSWORD_STRING = "****************"
user_email = None
user_password = None

class APIError(Exception):
    """Custom exception for API errors."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

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
        name = v1_config.get("name", "") + "_v2"

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
        v2_config["name"] = name

        return v2_config
    except Exception as e:
        raise Exception(f"Error transforming V1 to V2 configuration: {e}") from e


def create_v2_config(v1_config):
    """
    Create a V2 configuration by transforming the V1 configuration and
    prompting for any missing or sensitive values.
    """
    try:
        # Transform the V1 configuration to V2
        v2_config = transform_v1_to_v2(v1_config)

        # Prompt user to input values for fields with "****************"
        for key, value in v2_config.items():
            if value == SCRAPPED_PASSWORD_STRING:
                while True:
                    user_input = input(f"Please enter the value for {key}: ").strip()
                    if user_input:
                        v2_config[key] = user_input
                        break
                    else:
                        print("Input cannot be empty. Please try again.")

        if v2_config:
            print("V2 connector's config created successfully.")
        else:
            raise Exception("Failed to create a valid V2 connector configuration.")

        return v2_config
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_auth_token(base_url, email=None, password=None):
    global user_email, user_password

    # Use provided credentials or get them interactively
    if email and password:
        user_email = email
        user_password = password
    elif not user_email or not user_password:
        user_email, user_password = get_credentials_input()

    url = base_url + "api/sessions"

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
        return token
    except json.JSONDecodeError:
        raise APIError("Failed to decode JSON while getting auth token", response_text=response.text)

def get_connector_config(base_url, env, lkc, connector_name):
    global auth_token, last_poll_time
    if (datetime.now() - last_poll_time).total_seconds() > 180:
        auth_token = get_auth_token(base_url)

    cookies = {'auth_token': auth_token}
    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors/{connector_name}"

    print(f"Get connector config URL: {url}")
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

    print(f"Get connector offsets URL: {url}")
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

    json_data = {
        'name': connector_name + '_v2',
        'config': configs,
        'offsets': offsets
    }

    url = f"{base_url}api/accounts/{env}/clusters/{lkc}/connectors"

    print(f"Create connector URL: {url}")
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
        print(f"Connector '{connector_name}' created successfully. Response: {json.dumps(json_response, indent=2)}")
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

    print(f"Get connector status URL: {url}")
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

def get_credentials_input():
    """Handle credentials input with file support."""
    print("\n" + "="*60)
    print("🔐 Confluent Cloud Credentials")
    print("="*60)
    print("Choose how you want to provide your credentials:")
    print("1. Environment variables - Set EMAIL and PASSWORD environment variables")
    print("2. File - Provide path to a JSON file containing credentials (RECOMMENDED)")
    print("3. Secure input - Enter credentials manually (password hidden)")
    print()
    print("SECURITY NOTE: Option 2 (file) is recommended to avoid password exposure in command history.")

    cred_choice = input("Choose option (1-3, default is 1): ").strip()

    if cred_choice == "2":
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
                        print(f"✅ Credentials loaded from: {cred_file_path}")
                        return email, password
                    else:
                        print("❌ Invalid credentials file format. Expected: {\"email\": \"...\", \"password\": \"...\"}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            return get_credentials_secure_input()
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON format in credentials file: {e}")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        return get_credentials_secure_input()
                except Exception as e:
                    print(f"❌ Error reading credentials file: {e}")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        return get_credentials_secure_input()
            else:
                print("❌ File not found. Please provide a valid file path.")
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
            print("✅ Credentials loaded from environment variables")
            print("⚠️  NOTE: Environment variables may be visible in process lists and command history.")
            return email, password
        else:
            print("❌ EMAIL and PASSWORD environment variables not set")
            print("Falling back to secure input...")
            return get_credentials_secure_input()

def get_credentials_secure_input():
    """Get credentials through secure user input (password hidden)."""
    print("\n📝 Secure Credentials Input")
    print("Your password will be hidden when typing.")

    email = input("Enter your Confluent Cloud email: ").strip()

    # Use getpass for secure password input (hidden)
    password = getpass.getpass("Enter your Confluent Cloud password: ")

    if email and password:
        print("✅ Credentials received securely")
        return email, password
    else:
        print("❌ Email and password cannot be empty")
        return get_credentials_secure_input()

def main():
    parser = argparse.ArgumentParser(description="Migrate HTTP V1 sink connector to V2.")
    parser.add_argument('--v1_connector', required=True, help='Name of the V1 connector')
    parser.add_argument('--environment', required=True, help='Environment ID')
    parser.add_argument('--cluster_id', required=True, help='Cluster ID')
    args = parser.parse_args()

    connector_name = args.v1_connector
    env = args.environment
    lkc = args.cluster_id
    base_url = "https://confluent.cloud/"

    try:
        print("🔐 Setting up Confluent Cloud authentication...")
        # Get credentials first
        global user_email, user_password
        user_email, user_password = get_credentials_input()

        # Get initial auth token
        global auth_token, last_poll_time
        auth_token = get_auth_token(base_url, user_email, user_password)
        last_poll_time = datetime.now()

        print("Fetching V1 connector's status...")
        status = get_connector_status(base_url, env, lkc, connector_name)
        print(f"Connector status for {connector_name}: {status}")
        if status != "PAUSED":
            user_input = input("The connector is not in a paused state. There might be some duplication in the end"
                               " system if you continue. Do you still want to proceed? (yes/no): ")
            if user_input.lower() != 'yes':
                print("Exiting the migration tool...")
                return

        print("Fetching v1 connector offsets...")
        offsets = get_connector_offsets(base_url, env, lkc, connector_name)

        print("Fetching V1 connector's config...")
        v1_config = get_connector_config(base_url, env, lkc, connector_name)

        print("Transforming V1 connector's config to V2...")
        v2_config = create_v2_config(v1_config)

        # Display the V2 configuration and ask for confirmation
        print("\nThe transformed V2 connector configuration is as follows:")
        print(json.dumps(v2_config, indent=4))
        user_input = input("Please review the above configuration. Do you want to proceed with creating the V2 connector? (yes/no): ")
        if user_input.lower() != 'yes':
            print("Exiting the migration tool without creating the V2 connector...")
            return

        print("Creating V2 connector with offset set to that of V1 connector...")
        send_create_request(base_url, env, lkc, connector_name, v2_config, offsets)

    except APIError as e:
        print(f"Encountered Error: {e}, Status Code: {e.status_code}, Response: {e.response_text}")
    except Exception as e:
        print(f"An error occurred while running the migration tool: {e}")

if __name__ == '__main__':
    main()
