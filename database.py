import logging
from uuid import uuid4

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker, Session

from data_sources import Stations, Regions, Trips

engine = create_engine('sqlite:///test.sqlite')
Base = declarative_base()
logger = logging.getLogger(__name__)


class Station(Base):
    __tablename__ = 'stations'

    id = Column(Integer, primary_key=True)
    external_id = Column(String(50), nullable=False)
    name = Column(String(200), nullable=False)
    short_name = Column(String(16), nullable=False)
    latitudes = Column(Float, nullable=False)
    longitudes = Column(Float, nullable=False)
    region_id = Column(Float)
    region_name = Column(String(16))
    capacity = Column(Integer)
    has_kiosk = Column(Boolean)


class Trip(Base):
    __tablename__ = 'trips'

    id = Column(String(50), primary_key=True)
    trip_duration = Column(Float)
    predicted_trip_duration = Column(Float)
    start_time = Column(DateTime)
    start_station_id = Column(Integer, ForeignKey('stations.id'))
    bike_id = Column(Integer)
    user_type = Column(String)
    birth_year = Column(Integer)
    gender = Column(Integer)
    actual_sent = Column(Boolean)

    start_station = relationship('Station')


class Database:
    def __init__(self):
        self.session: Session

    @staticmethod
    def create_table():
        Base.metadata.create_all(engine)

    @staticmethod
    def create_index():
        pass

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
                region_name=regions[item['region_id']],
                capacity=item['capacity'],
                has_kiosk=item['has_kiosk']
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

    def get_trip_data_without_predictions(self, start_time_range) -> [Trip]:
        return self.session.query(Trip).join(Station).filter(
            Trip.start_time.between(start_time_range[0], start_time_range[1]),
            Trip.predicted_trip_duration.is_(None)
        ).limit(10).all()

    def update_predicted_trip_duration(self, updates):
        self.session.query(Trip).update({
            Trip.predicted_trip_duration: case(
                updates, value=Trip.id
            )
        }, synchronize_session=False)

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
