import database
from data_sources import Stations, Regions, Trips
from processor.dumper import Dumper


if __name__ == '__main__':
    database.create_table_if_not_exist()

    # update data stored in the system
    stations = Stations()
    regions = Regions()
    trips = Trips('data/201908-bluebikes-tripdata.csv')
    with Dumper() as dumper:
        dumper.update_stations(stations, regions)
        dumper.update_trip_data(trips)
