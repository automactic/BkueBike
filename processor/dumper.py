from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from database import engine, Station, Trip
from data_sources import Stations, Regions, Trips
from uuid import uuid4


class Dumper:
    def __init__(self):
        self.session: Session

    def update_stations(self, stations: Stations, regions: Regions):
        for item in stations.all:
            station = Station(
                id=int(item['station_id']),
                external_id=item['external_id'],
                name=item['name'],
                short_name=item['short_name'],
                latitudes=item['lat'],
                longitudes=item['lon'],
                region_id=item['region_id'],
                region_name=regions[item['region_id']]
            )
            self.session.merge(station)

    def update_trip_data(self, trips: Trips):
        if self._trip_exists_with_start_date(trips.first_trip_start_time):
            return

        batch = []
        for index, item in trips.dataframe.iterrows():
            trip = Trip(
                id=str(uuid4()),
                trip_duration=item['trip_duration'],
                start_time=item['start_time'],
                start_station_id=item['start_station_id'],
                bike_id=item['bike_id'],
                user_type=item['user_type'],
                birth_year=item['birth_year'],
                gender=item['gender'],
            )
            batch.append(trip)
            if index % 1000 == 0:
                self.session.add_all(batch)
                batch = []

    def _trip_exists_with_start_date(self, start_time):
        trip = self.session.query(Trip).filter_by(start_time=start_time).first()
        return trip is not None

    def __enter__(self):
        Session = sessionmaker(bind=engine)
        self.session = Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.session.commit()
        except:
            self.session.rollback()
            raise
        finally:
            self.session.close()