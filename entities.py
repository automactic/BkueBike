from dataclasses import dataclass


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