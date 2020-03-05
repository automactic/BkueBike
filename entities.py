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
    latitude: float
    longitude: float
    region_name: str = None
    capacity: int = None
    has_kiosk: bool = None


@dataclass
class Trip:
    id: str
    trip_duration: float
    start_station_id: str
    end_station_id: str
    start_time: datetime
    stop_time: datetime
    bike_id: int
    user_type: str
    user_birth_year: int
    user_gender: str
    predicted_trip_duration: float = None
    submitted_actual: bool = False
