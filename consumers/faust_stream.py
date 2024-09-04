"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO (done): Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
station_topic = app.topic("cta.stations", value_type=Station)
# TODO (done): Define the output Kafka Topic
transformed_station_topic = app.topic("cta.stations.transformed", partitions=1)
# TODO (done): Define a Faust Table
table = app.Table(
    "transformed_stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=transformed_station_topic,
)


# TODO (done): Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
@app.agent(station_topic)
async def process_station(stations):
    async for station in stations:
        # Determine the line color based on which field is True
        line_color = None
        if station.red:
            line_color = "red"
        elif station.blue:
            line_color = "blue"
        elif station.green:
            line_color = "green"

        # Create a TransformedStation record
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_color
        )

        # Send the transformed record to the output topic
        await transformed_station_topic.send(value=transformed_station)


if __name__ == "__main__":
    app.main()
