# Confluent Connectors Migration Tool

This repository provides migration tools to assist in transitioning various Confluent Cloud connectors to upgraded versions. The tools help automate the migration process while preserving connector configurations and ensuring data continuity.

## Available Migration Tools

### HTTP Connectors
- **HTTP V2 Sink Migration**: Migrate HTTP V1 sink connectors to the new HTTP V2 sink connectors.

### BigQuery Connectors
- **BigQuery Legacy to Storage Write API**: Migrate BigQuery Legacy sink connectors to the new 
  Storage Write API (V2) connectors.
## Getting Started

Each migration tool is located in its respective directory with detailed instructions:

- [`http-v2-sink/`](http-v2-sink/) - HTTP V1 to V2 sink connector migration
- [`bigquery-v2-sink/`](bigquery-v2-sink/) - BigQuery V1 to V2 sink connector migration

## General Migration Process

1. **Pause the V1 connector** in Confluent Cloud
2. **Set up environment variables** for authentication
3. **Run the migration tool** with appropriate parameters
4. **Review and confirm** the upgraded connector configuration
5. **Monitor** the new connector after migration

## Important Notes

- Migration tools preserve offsets to prevent data duplication
- Always test migrations with small datasets first
- Review breaking changes specific to each connector type
- Monitor the new connectors for any issues after migration

## Contributing

This repository is designed to support migration tools for various Confluent connectors. Contributions for additional connector types are welcome.
