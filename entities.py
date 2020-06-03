from dataclasses import dataclass
from datetime import datetime

@dataclass
class Region:
    id: str
    name: str


@dataclass
class Station:
    id: str
    name: str
    short_name: str
    latitude: float
    longitude: float
    region_id: str
    region_name: str
    capacity: int
    has_kiosk: bool


@dataclass
class Trip:
    id: str
    trip_duration: float
    predicted_trip_duration: float
    start_station_id: str
    end_station_id: str
    start_time: datetime
    end_time: datetime
    bike_id: int
    user_type: str
    user_birth_year: int
    user_gender: str
    submitted_actual: bool
