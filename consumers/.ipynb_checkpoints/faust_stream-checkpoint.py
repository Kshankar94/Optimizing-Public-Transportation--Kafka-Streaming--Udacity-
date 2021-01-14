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



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("connect-postgres-stations", value_type=Station)
out_topic = app.topic("transformed-postgres-station-topic", partitions=1, value_type = TransformedStation)

transformed_station_table = app.Table(
   "transformed-station-table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)



# transformed input `Station` records into `TransformedStation` records. 
# "line" is the color of the station. if the`Station` record   field `red` set to true, set the `line` of the `TransformedStation` record to the string `"red"`
#
@app.agent(topic)
async def transform_stations(station_events):
    async for station_event in station_events:
        if station_event.red == True:
            line = "red"
        elif station_event.blue == True:
            line = "blue"
        elif station_event.green == True:
            line = "green"
        else:
            line = "Nil"
         
        #logger.info(f"station_id is {station_event.station_id} and station_name is {station_event.station_name}")
        
        transformed_station_table[station_event.station_id] = TransformedStation(station_id = station_event.station_id, station_name = station_event.station_name, order = station_event.order, line = line)
        
        


if __name__ == "__main__":
    app.main()
