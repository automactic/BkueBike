# from datarobot import Deployment, PredictionServer
from processor.training import TrainingData

from database import Database
from scoring import Scoring
from data_sources import Stations, Regions, Trips


def import_data():
    stations = Stations()
    regions = Regions()
    trip_csvs = ['data/201908-bluebikes-tripdata.csv']
    with Database() as database:
        database.update_stations(stations, regions)
        for trip_csv in trip_csvs:
            trips = Trips(trip_csv)
            database.update_trip_data(trips)

if __name__ == '__main__':
    # TrainingData('data/201907-bluebikes-tripdata.csv').process()
    Database.create_table()

    import_data()

    # scoring = Scoring()
    # scoring.select_data()

    # create_deployment()
