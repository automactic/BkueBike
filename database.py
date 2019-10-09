from sqlalchemy import *
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine('sqlite:///test.sqlite')
Base = declarative_base()


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


def create_table_if_not_exist():
    Base.metadata.create_all(engine)
