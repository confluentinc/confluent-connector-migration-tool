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
| **Behavior on Malformed Docs** | V2 only supports `ignore` and `fail`. The value `warn` will be automatically converted to `ignore` |

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
5. **Migration Mode Selection**:
   - **PRODUCTION**: Creates a V2 connector with similar configuration to your V1 connector (recommended)
   - **TEST**: Allows you to test with a subset of topics by providing custom topic-to-resource mapping
6. **New Connector Name**: Choose a name for the V2 connector
7. **Elasticsearch Server Version**: Select V8 or V9
8. **Authentication Type**: Choose BASIC, NONE, or API_KEY
9. **Configuration Review**: Review the final V2 configuration before creation

### 6. Verify and Cleanup

After successful migration:

1. Verify the V2 connector is running in Confluent Cloud Console
2. Check that data is being written to Elasticsearch correctly
3. Monitor connector metrics for any errors
4. Once verified, delete the old V1 connector

## Migration Modes

The tool supports two migration modes:

### PRODUCTION Mode (Recommended)

Use this for standard migrations:
- Your new V2 connector will have similar configuration to the V1 connector
- `auto.create` and `resource.type` are automatically derived from your V1 configuration
- Topic-to-resource mapping (if configured in V1) is automatically copied

**Recommended for**: Final production migrations

### TEST Mode

Use this to validate the migration before committing:
- Allows you to test with a subset of topics
- Requires you to provide topic-to-resource mapping explicitly
- Helps verify data flow and configuration before production migration
- `auto.create` is set to `false` to ensure resources exist before writing

**Recommended for**: Testing migrations with non-critical data

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

## ⚠️ SSL File Configurations

If your V1 connector has SSL enabled and you have uploaded SSL certificate files (keystore/truststore), be aware of the following:

### SSL Files Not Migrated

This migration tool **does not upload SSL certificate files** to the V2 connector. This includes:

- `elastic.https.ssl.truststore.file` - Truststore with CA certificates
- `elastic.https.ssl.keystore.file` - Keystore with client certificates

### Manual Upload Required

If your V1 connector uses SSL files:

1. **Create the V2 connector** using this migration tool
2. **Manually upload SSL files** via Confluent Cloud UI:
   - Go to Connectors > Your V2 Connector > Settings
   - Locate the SSL certificate fields
   - Upload the keystore/truststore files through the Cloud Console
3. **Resume the V2 connector** after uploading files

### Identifying SSL File Usage

To check if your V1 connector has SSL files:

```bash
# Use Confluent CLI to describe the connector
confluent connect cluster describe <connector-id>

# Look for these config keys with non-empty values:
# - elastic.https.ssl.truststore.file
# - elastic.https.ssl.keystore.file
```

If these configs are present and have values (shown as `*` for security), you'll need to manually upload the files after migration.

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

### Data and Offsets

<details>
<summary><strong>Will there be data loss during migration?</strong></summary>

No. The tool preserves V1 connector offsets and applies them to the V2 connector, ensuring no data is lost or duplicated.

</details>

<details>
<summary><strong>Can I run both V1 and V2 connectors simultaneously?</strong></summary>

Yes, but this may cause data duplication. This is useful for testing but not recommended for production. To avoid duplication in production:
- Pause the V1 connector before running the V2 connector
- Once verified, delete the V1 connector

</details>

### Configuration and Migration

<details>
<summary><strong>What if my V1 connector uses multiple Elasticsearch URLs?</strong></summary>

V2 only supports a single URL. The tool will detect multiple URLs and prompt you to enter which URL to use during migration.

</details>

<details>
<summary><strong>What happens to my SMT (Single Message Transform) configurations?</strong></summary>

All `transforms.*` configurations are automatically copied to the V2 connector without modification.

</details>

<details>
<summary><strong>What's the difference between PRODUCTION and TEST migration modes?</strong></summary>

- **PRODUCTION Mode**: Migrates your connector with auto-derived settings. Recommended for final migration. Auto-creates settings based on your V1 configuration.
- **TEST Mode**: Requires you to manually specify topic-to-resource mapping. Useful for validating the migration before production with a subset of topics.

</details>

### Automatic Conversions

<details>
<summary><strong>Why isn't `data.stream.type: NONE` showing as a breaking change?</strong></summary>

The tool automatically converts V1's `data.stream.type: NONE` to V2's default `LOGS` (when applicable). This is handled transparently during migration, so you don't need to take any action.

</details>

<details>
<summary><strong>Why isn't the default batch size difference (`2000` vs `50`) a breaking change?</strong></summary>

The tool preserves your current batch size setting from the V1 connector, so the default difference doesn't affect you. If you explicitly set a batch size in V1, that exact value will be used in V2.

</details>

<details>
<summary><strong>What happens to `behavior.on.malformed.documents: warn`?</strong></summary>

V2 only supports `ignore` and `fail` values. The tool automatically converts `warn` to `ignore` during migration. This is transparent and requires no action from you.

</details>

### Rollback and Recovery

<details>
<summary><strong>How do I roll back if something goes wrong?</strong></summary>

Since the original V1 connector is not modified by the migration tool:
1. Delete the V2 connector in Confluent Cloud
2. Resume the V1 connector
3. Your data and offsets remain intact

</details>
