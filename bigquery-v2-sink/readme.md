# BigQuery Legacy to Storage Write API Migration Tool

This tool migrates BigQuery Legacy sink connectors to the new Storage Write API (V2) connectors in Confluent Cloud. The V2 connector provides improved performance, cost efficiency, and better data type support.

## ⚠️ Important Breaking Changes

The Storage Write API has several breaking changes from the Legacy InsertAll API:

- **TIMESTAMP**: Values are now interpreted as microseconds since epoch instead of seconds
- **DATE**: Now supports INT values in range -719162 to 2932896 (not supported in Legacy API)
- **DATETIME_FORMAT**: Supports only a subset of datetime canonical formats
- **DATA_TYPES**: Different data type support compared to Legacy API
- **INT8/INT16**: Now cast to FLOAT type by default (can be configured to INTEGER)

**Recommendations:**
1. Test the migration with a small dataset first
2. Verify data integrity after migration
3. Review the [official documentation](https://docs.confluent.io/cloud/current/connectors/cc-gcp-bigquery-storage-sink.html#legacy-to-v2-connector-migration)

## Prerequisites

- Python 3.6 or higher
- `requests` library (`pip install requests`)
- Confluent Cloud account with access to the legacy connector
- GCP service account with BigQuery permissions

## Steps to Run the Migration Tool

### 1. Pause the V1 Sink Connector
Pause your BigQuery Legacy sink connector in Confluent Cloud. You will get a warning (can proceed after confirmation) if the connector is not paused.

### 2. Get Environment Details
Fetch the environment name and cluster ID from your Kafka cluster's URL in Confluent Cloud.

### 3. Set Environment Variables
Set your Confluent Cloud credentials as environment variables:
```bash
export EMAIL="your-email@example.com"
export PASSWORD="your-password"
```

### 4. Run the Migration Tool
```bash
python3 migrate_bigquery_connector.py --legacy_connector "<YOUR_LEGACY_CONNECTOR_NAME>" --environment "<YOUR_ENVIRONMENT_NAME>" --cluster_id "<YOUR_KAFKA_CLUSTER_ID>"
```

### 5. Follow the Interactive Prompts
The tool will guide you through:
- **Connector Name**: Choose a name for the new V2 connector
- **Ingestion Mode**: Select from STREAMING, BATCH LOADING, UPSERT, or UPSERT_DELETE
- **Int8/Int16 Casting**: Choose between FLOAT (default) or INTEGER
- **Commit Interval**: For BATCH LOADING mode (60-14400 seconds)
- **Auto Create Tables**: Configure table creation strategy
- **Partitioning**: Set partitioning type and field (if applicable)
- **Date Time Formatter**: Choose between SimpleDateFormat or DateTimeFormatter
- **GCP Keyfile**: Provide service account credentials

### 6. Review and Confirm
The tool will show the final V2 connector configuration and ask for confirmation before creating the connector.

## Configuration Options

### Ingestion Modes
- **STREAMING**: Lower latency, higher cost (default)
- **BATCH LOADING**: Higher latency, lower cost
- **UPSERT**: For upsert operations (requires key fields)
- **UPSERT_DELETE**: For upsert and delete operations (requires key fields)

### Auto Create Tables Options
- **DISABLED**: Don't auto-create tables
- **NON-PARTITIONED**: Create tables without partitioning
- **PARTITION by INGESTION TIME**: Create time-partitioned tables
- **PARTITION by FIELD**: Create field-partitioned tables

### Partitioning Types
- **HOUR**: Partition by hour
- **DAY**: Partition by day
- **MONTH**: Partition by month
- **YEAR**: Partition by year

## GCP Service Account Keyfile

The tool supports three methods for providing your GCP service account keyfile:

1. **File Path**: Provide the path to your JSON keyfile
2. **Environment Variable**: Set `GCP_KEYFILE_PATH` environment variable
3. **Direct Input**: Paste the JSON content directly

## Important Notes

- **Offset Preservation**: The V2 connector will start from the latest offset of your V1 connector
- **Data Loss Prevention**: Offsets are automatically transferred to avoid data duplication
- **Breaking Changes**: Review the breaking changes section above before migration
- **Testing**: Always test with a small dataset first
- **Monitoring**: Monitor the new connector for any issues after migration

## Example Usage

```bash
# Set environment variables
export EMAIL="user@example.com"
export PASSWORD="your-password"

# Run migration
python3 migrate_bigquery_connector.py \
  --legacy_connector "my-legacy-bigquery-connector" \
  --environment "env-123456" \
  --cluster_id "lkc-abc123"
```

The tool will guide you through the migration process interactively, ensuring a smooth transition from Legacy to Storage Write API connectors.