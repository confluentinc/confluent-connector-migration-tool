# Confluent Connectors Migration Tool

This repository provides migration tools to assist in transitioning various Confluent Cloud connectors to upgraded versions. The tools help automate the migration process while preserving connector configurations and ensuring data continuity.

## Available Migration Tools

### HTTP Connectors
- **HTTP V2 Sink Migration**: Migrate HTTP V1 sink connectors to the new HTTP V2 sink connectors.

### BigQuery Connectors
- **BigQuery Legacy to Storage Write API**: Migrate BigQuery Legacy sink connectors to the new
  Storage Write API (V2) connectors.

### Elasticsearch Connectors
- **Elasticsearch V2 Sink Migration**: Migrate Elasticsearch Sink V1 connectors to the new
  Elasticsearch Sink V2 connectors with improved HTTP framework.

## Getting Started

Each migration tool is located in its respective directory with detailed instructions:

- [`http-v2-sink/`](http-v2-sink/) - HTTP V1 to V2 sink connector migration
- [`bigquery-v2-sink/`](bigquery-v2-sink/) - BigQuery V1 to V2 sink connector migration
- [`elasticsearch-v2-sink/`](elasticsearch-v2-sink/) - Elasticsearch V1 to V2 sink connector migration

## General Migration Process

1. **Pause the V1 connector** in Confluent Cloud (optional but recommended)
2. **Provide Confluent Cloud credentials** via:
   - Credentials file (recommended): JSON file with `email` and `password` fields
   - Environment variables: `EMAIL` and `PASSWORD`
   - Secure interactive input: The script will prompt with hidden password
3. **Run the migration tool** with appropriate parameters
4. **Review and confirm** the upgraded connector configuration
5. **Monitor** the new connector after migration

## Example Usage

### Elasticsearch V1 to V2 Migration

```bash
# Set credentials in a file
cat > ~/credentials/confluent_creds.json << 'EOF'
{
  "email": "your-email@confluent.io",
  "password": "your-password"
}
EOF
chmod 600 ~/credentials/confluent_creds.json

# Run migration
python3 elasticsearch-v2-sink/migrate-to-elasticsearch-v2-sink.py \
  --v1_connector <connector-name> \
  --environment <env-id> \
  --cluster_id <cluster-id>

# When prompted for credentials, choose option 2 (File) and provide:
# ~/credentials/confluent_creds.json
```

## Architecture

### MigrationClient Class

The migration tools use a `MigrationClient` class (in `utils/migration_utils.py`) for authentication and API operations:

- **Instance-based state**: Each client instance maintains its own authentication token and credentials
- **Thread-safe**: Multiple clients can run concurrently without interfering with each other
- **Token refresh**: Automatically refreshes authentication tokens when needed (3+ minute idle)

### Helper Functions

Stateless utility functions for user interactions:
- `get_credentials_input()`: Interactive credential collection with multiple input options
- `prompt_for_sensitive_values()`: Prompt for masked configuration values
- `display_config_and_confirm()`: Display and confirm final configuration

## Important Notes

- Migration tools preserve offsets to prevent data duplication
- Always test migrations with non-production connectors first
- Review breaking changes specific to each connector type
- Monitor the new connectors for any issues after migration
- Use credentials files instead of environment variables to avoid exposing passwords in shell history

## Contributing

This repository is designed to support migration tools for various Confluent connectors. Contributions for additional connector types are welcome.
