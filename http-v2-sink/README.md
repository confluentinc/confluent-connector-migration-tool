Steps to run the migration tool.
  1. Pause the V1 sink connector. You will get a warning (can proceed after confirmation) while running this tool if the connector is not paused.
  2. Fetch the environment name and cluster id (from your kafka cluster's URL)
  3. Set environment variables EMAIL and PASSWORD on your terminal. These are the credentials you use to login on Confluent Cloud UI.
  4. Run the tool after adding appropriate values:
     python3 migrate-to-http-v2-sink.py --v1_connector "<YOUR_V1_CONNECTOR_NAME>" --environment "<YOUR_ENVIRONMENT_NAME>" --cluster_id "<YOUR_KAFKA_CLUSTER_ID>"
  5. The tool might ask you for a few secrets which you've had in the V1 connector. It will also ask for confirmation before creating the V2 connector. 

Note: The V2 connector will start from the latest offset of your V1 connector.
