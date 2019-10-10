# from datarobot import Deployment, PredictionServer
from processor.training import TrainingData

from database import Database
from scoring import Scoring
from data_sources import Stations, Regions, Trips


def import_and_update_database():
    stations = Stations()
    regions = Regions()
    trip_csv_paths = ['data/201908-bluebikes-tripdata.csv']
    with Database() as database:
        database.update_stations(stations, regions)
        for trip_csv_path in trip_csv_paths:
            trips = Trips(trip_csv_path)
            database.update_trip_data(trips)


if __name__ == '__main__':
    # TrainingData('data/201907-bluebikes-tripdata.csv').process()
    Database.create_table()

    import_and_update_database()

    scoring = Scoring()
    scoring.predict()
