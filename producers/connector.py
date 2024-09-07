"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations-jdbc-connect"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO (done): Complete the Kafka Connect Config below.
    # Use the JDBC Source Connector to connect to Postgres. Load the `stations` table using incrementing mode, with `stop_id` as the incrementing column name.

    # Define the connector configuration
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "batch.max.rows": "500",  # Maximum number of rows to include in a single batch
            "connection.url": "jdbc:postgresql://postgres:5432/cta",  # Connection URL for the Postgres database (Docker)
            "connection.user": "cta_admin",  # Postgres username
            "connection.password": "chicago",  # Postgres password
            "table.whitelist": "stations",  # Only capture data from the `stations` table
            "mode": "incrementing",  # Use incrementing mode for capturing changes
            "incrementing.column.name": "stop_id",  # Use `stop_id` as the incrementing column
            "topic.prefix": "cta.",  # Prefix for Kafka topics, resulting in `cta_stations_stations`
            "poll.interval.ms": "3600000",  # Poll the database every hour (3600000 milliseconds)
        }
    }

    # Post the configuration to Kafka Connect
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config)
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
