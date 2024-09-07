"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO (done): Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        # TODO (done): Complete the below by deciding on a topic name, number of partitions, and number of replicas
        #
        super().__init__(
            f"cta.turnstile_events", # TODO (done): come up with an appropriate topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1, # Default value, change if needed
            num_replicas=1, # Default value, change if needed
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        #
        # TODO (done): Complete this function by emitting a message to the turnstile topic for the number of entries that were calculated
        #

        line_color = None
        
        # Map the string representation of color to the actual color name string
        if self.station.color.name == "blue":
            line_color = "blue"
        if self.station.color.name == "red":
            line_color = "red"
        if self.station.color.name == "green":
            line_color = "green"

        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station_name,
                    "line": line_color,
                },
            )

        logger.info(f"Produced {num_entries} entries for station {self.station_name}")
