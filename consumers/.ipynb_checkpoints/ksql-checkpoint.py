"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#ksql statement to create turnstile table and turnstile_summary_table

KSQL_STATEMENT = """
CREATE TABLE turnstile_table
  (station_name VARCHAR,
   station_id INTEGER,
   line VARCHAR)
  WITH (KAFKA_TOPIC='com.cta.turnstile.events',
        VALUE_FORMAT='AVRO',
        KEY='station_id');
        
CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    SELECT station_id, COUNT(station_id) FROM turnstile_table GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to Ksql {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to ksql {json.dumps(resp.json(), indent=2)}")


if __name__ == "__main__":
    execute_statement()
