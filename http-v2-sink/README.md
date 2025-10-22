# HTTP V2 Sink Connector Migration Tool

This tool migrates HTTP V1 sink connector to the HTTP V2 sink connector in Confluent Cloud.


## Steps to Run the Migration Tool

1. **Pause the V1 sink connector**. You will get a warning (can proceed after confirmation) while running this tool if the connector is not paused.

2. **Fetch the environment id and cluster id** (from your kafka cluster's URL)

3. **Set Credentials**. The tool supports three methods for providing your Confluent Cloud credentials:

### Option 1: Environment Variables
```bash
export EMAIL="your-email@example.com"
export PASSWORD="your-password"
```
⚠️ **Security Note**: Environment variables may be visible in process lists and command history.

### Option 2: Credentials File (RECOMMENDED)
Create a JSON file with your credentials:
```json
{
  "email": "your-email@example.com",
  "password": "your-password"
}
```

### Option 3: Secure Input
Enter credentials interactively when prompted. The password will be hidden when typing.

4. **Run the tool** after adding appropriate values:
   ```bash
   python3 migrate-to-http-v2-sink.py --v1_connector "<YOUR_V1_CONNECTOR_NAME>" --environment <YOUR_ENVIRONMENT_ID> --cluster_id "<YOUR_KAFKA_CLUSTER_ID>"
   ```

5. **Follow the interactive prompts**. The tool will guide you through:
   - **Credentials**: Choose secure method for Confluent Cloud authentication
   - **Connector Status**: Review current V1 connector status and proceed with confirmation
   - **Configuration Review**: Review the transformed V2 configuration before creation
   - **Confirmation**: Confirm before creating the V2 connector

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

## Example Credentials File

Create a file named `credentials.json`:
```json
{
  "email": "user@example.com",
  "password": "your-password"
}
```

## Related Documentation

- [HTTP V2 Sink Connector Documentation](https://docs.confluent.io/cloud/current/connectors/cc-http-sink-v2.html)
