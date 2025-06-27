# HTTP V2 Sink Connector Migration Tool

This tool migrates HTTP V1 sink connector to the HTTP V2 sink connector in Confluent Cloud. The V2 connector provides improved performance, better error handling, and enhanced configuration options.


## Steps to Run the Migration Tool

1. **Pause the V1 sink connector**. You will get a warning (can proceed after confirmation) while running this tool if the connector is not paused.

2. **Fetch the environment name and cluster id** (from your kafka cluster's URL)

3. **Set environment variables EMAIL and PASSWORD** on your terminal. These are the credentials you use to login on Confluent Cloud UI.

4. **Run the tool** after adding appropriate values:
   ```bash
   python3 migrate-to-http-v2-sink.py --v1_connector "<YOUR_V1_CONNECTOR_NAME>" --environment "<YOUR_ENVIRONMENT_NAME>"
     --cluster_id "<YOUR_KAFKA_CLUSTER_ID>"
   ```

5. **Follow the interactive prompts**. The tool might ask you for a few secrets which you've had in the V1 connector. It will also ask for confirmation before creating the V2 connector.

## Important Notes

- **Offset Preservation**: The V2 connector will start from the latest offset of your V1 connector
- **Testing**: Always test with a small dataset first
- **Monitoring**: Monitor the new connector for any issues after migration

## Example Usage

```bash
# Set environment variables
export EMAIL="user@example.com"
export PASSWORD="your-password"

# Run migration
python3 migrate-to-http-v2-sink.py \
  --v1_connector "my-http-v1-sink" \
  --environment "env-123456" \
  --cluster_id "lkc-abc123"
```

## Related Documentation

- [HTTP V2 Sink Connector Documentation](https://docs.confluent.io/cloud/current/connectors/cc-http-sink-v2.html)
