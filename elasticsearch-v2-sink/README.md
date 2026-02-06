# Elasticsearch Sink V1 to V2 Migration Tool

This tool migrates Elasticsearch Sink V1 connectors to V2 connectors in Confluent Cloud.

## Important: Architectural Differences

V1 and V2 are architecturally different connectors:
- **V1** uses the Elasticsearch SDK
- **V2** uses a modern HTTP framework

Direct in-place upgrade is not possible. This tool creates a **new V2 connector** with preserved offsets from the V1 connector.

## Breaking Changes

The V2 connector has several breaking changes from V1:

| Change | Description |
|--------|-------------|
| **Connection URL** | V1 supported multiple URLs (comma-separated), V2 supports only a single URL |
| **DELETE Handling** | V2 requires a valid document `_id` for DELETE operations |
| **Error Routing** | V2 uses error topics instead of DLQ |
| **Resource Creation** | V2 creates resources and mappings together when `auto.create=true` |
| **key.ignore Type** | V1 uses STRING, V2 uses BOOLEAN (auto-converted during migration) |

### New V2 Properties

The following properties are new in V2 and require user input during migration:

| Property | Description | Options |
|----------|-------------|---------|
| `elastic.server.version` | Elasticsearch server version | V8, V9 |
| `auth.type` | Authentication method | NONE, BASIC, API_KEY |
| `api.key.value` | API key (if auth.type=API_KEY) | string |
| `elastic.ssl.enabled` | Enable SSL | true/false (derived from `elastic.security.protocol`) |
| `auto.create` | Auto-create resources | true/false (derived from `external.resource.usage`) |
| `resource.type` | Target resource type | INDEX, DATASTREAM, ALIAS_INDEX, ALIAS_DATASTREAM |

### Discontinued V1 Properties

The following V1 properties are not supported in V2 and will be dropped with a warning:

| Property | Reason |
|----------|--------|
| `drop.invalid.message` | V2 fails task on preprocessing errors |
| `linger.ms` | V2 uses framework-level batching |
| `flush.timeout.ms` | V2 uses framework-level timeout |
| `type.name` | Deprecated in ES7+, not needed |
| `external.resource.usage` | Split into `auto.create` + `resource.type` |

### Renamed Properties

| V1 Property | V2 Property |
|-------------|-------------|
| `topic.to.external.resource.mapping` | `topic.to.resource.mapping` |

## Prerequisites

- Python 3.7+
- `requests` library (`pip install requests`)
- Confluent Cloud account with access to the V1 connector
- V1 connector should be **PAUSED** for production migrations (to avoid data duplication)

## Installation

```bash
# Install required dependencies
pip install requests

# Clone the repository (if not already done)
cd confluent-connector-migration-tool
```

## Quick Start

```bash
python3 elasticsearch-v2-sink/migrate-to-elasticsearch-v2-sink.py \
  --v1_connector "your-connector-name" \
  --environment "env-xxxxx" \
  --cluster_id "lkc-xxxxx"
```

## Steps to Run the Migration Tool

### 1. Get Environment Details

Fetch the environment ID and cluster ID from your Kafka cluster's URL in Confluent Cloud:
- Environment ID: `env-xxxxx`
- Cluster ID: `lkc-xxxxx`

### 2. Pause the V1 Connector (Recommended for Production)

Before migrating production connectors, pause the V1 connector to avoid data duplication:

```bash
# Using Confluent Cloud Console
# Navigate to Connectors > Your Connector > Pause
```

### 3. Set Credentials

The tool supports three methods for providing your Confluent Cloud credentials:

#### Option 1: Environment Variables
```bash
export EMAIL="your-email@example.com"
export PASSWORD="your-password"
```

#### Option 2: Credentials File (RECOMMENDED)
Create a JSON file with your credentials:
```json
{
  "email": "your-email@example.com",
  "password": "your-password"
}
```

#### Option 3: Secure Input
Enter credentials interactively when prompted. The password will be hidden when typing.

### 4. Run the Migration Tool

```bash
python3 elasticsearch-v2-sink/migrate-to-elasticsearch-v2-sink.py \
  --v1_connector "my-elasticsearch-connector" \
  --environment "env-abc123" \
  --cluster_id "lkc-xyz789"
```

### 5. Follow the Interactive Prompts

The tool will guide you through:

1. **Breaking Changes Warning**: Review and acknowledge the breaking changes
2. **Authentication**: Provide your Confluent Cloud credentials
3. **Connector Status Check**: Verify the V1 connector status
4. **Discontinued Configs**: Review any V1 configs that won't be migrated
5. **New Connector Name**: Choose a name for the V2 connector
6. **Elasticsearch Server Version**: Select V8 or V9
7. **Authentication Type**: Choose BASIC, NONE, or API_KEY
8. **Configuration Review**: Review the final V2 configuration before creation

### 6. Verify and Cleanup

After successful migration:

1. Verify the V2 connector is running in Confluent Cloud Console
2. Check that data is being written to Elasticsearch correctly
3. Monitor connector metrics for any errors
4. Once verified, delete the old V1 connector

## Configuration Options

### Authentication Types

| Type | Description | Required Credentials |
|------|-------------|---------------------|
| BASIC | Username & password authentication | `connection.username`, `connection.password` |
| NONE | No authentication | None |
| API_KEY | API key authentication (new in V2) | `api.key.value` |

### Resource Types

| Type | Description |
|------|-------------|
| INDEX | Write to Elasticsearch indices |
| DATASTREAM | Write to Elasticsearch data streams |
| ALIAS_INDEX | Write to index aliases |
| ALIAS_DATASTREAM | Write to data stream aliases |

## Example Usage

### Basic Migration

```bash
# Set credentials via environment variables
export EMAIL="user@example.com"
export PASSWORD="your-password"

# Run migration
python3 elasticsearch-v2-sink/migrate-to-elasticsearch-v2-sink.py \
  --v1_connector "es-sink-prod" \
  --environment "env-123456" \
  --cluster_id "lkc-abc123"
```

### Using Credentials File

```bash
# Create credentials file
cat > credentials.json << EOF
{
  "email": "user@example.com",
  "password": "your-password"
}
EOF

# Run migration (select option 2 when prompted for credentials)
python3 elasticsearch-v2-sink/migrate-to-elasticsearch-v2-sink.py \
  --v1_connector "es-sink-prod" \
  --environment "env-123456" \
  --cluster_id "lkc-abc123"
```

## Troubleshooting

### Common Issues

**Authentication Failed**
- Verify your email and password are correct
- Ensure you have access to the environment and cluster

**Connector Not Found**
- Check the connector name is spelled correctly
- Verify the environment and cluster IDs

**V1 Connector is RUNNING**
- For production migrations, pause the V1 connector first
- For testing, you can proceed with both connectors running (may cause data duplication)

**API Key Authentication**
- When using API_KEY auth type, you'll be prompted to enter the API key value
- The API key must have appropriate permissions for your Elasticsearch cluster

## FAQ

**Q: Will there be data loss during migration?**
A: No. The tool preserves V1 connector offsets and applies them to the V2 connector, ensuring no data is lost or duplicated.

**Q: Can I run both V1 and V2 connectors simultaneously?**
A: Yes, but this may cause data duplication. This is useful for testing but not recommended for production.

**Q: What if my V1 connector uses multiple Elasticsearch URLs?**
A: V2 only supports a single URL. The tool will detect multiple URLs and prompt you to enter which URL to use.

**Q: How do I roll back if something goes wrong?**
A: Since the original V1 connector is not modified, you can simply delete the V2 connector and resume the V1 connector.

**Q: What happens to my SMT (Single Message Transform) configurations?**
A: All `transforms.*` configurations are automatically copied to the V2 connector.
