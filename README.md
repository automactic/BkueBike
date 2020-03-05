# BlueBike trip duratiuon prediction
Predict how long a BlueBike trip is gonna last at the beginning of the trip.

This repo is a dockerized application that drives the prediction activities. 
The application is a asyncio python program with three components:

- data import (from json and CSV)
- scoring
- submit actuals

Required feature to make predictions:
- trip_id
- bike_id
- birth_year
- gender
- start_station_id
- start_station_name
- end_station_name
- start_time
- station_capacity
- station_has_kiosk
- station_region_id
- user_type