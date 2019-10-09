import pandas as pd

from data_sources import Regions, Stations


class TrainingData:
    def __init__(self, source_file_path):
        self.stations = Stations()
        self.regions = Regions()
        self.source_file_path = source_file_path

    def process(self):
        dataframe = pd.read_csv(self.source_file_path)
        dataframe = dataframe.rename(columns={
            'tripduration': 'trip_duration',
            'starttime': 'start_time',
            'bikeid': 'bike_id',
            'usertype': 'user_type',
            'start station id': 'start_station_id',
            'birth year': 'birth_year',
        })
        dataframe = dataframe.drop(columns=[
            'stoptime',
            'start station name',
            'start station latitude',
            'start station longitude',
            'end station id',
            'end station name',
            'end station latitude',
            'end station longitude'
        ])
        stations = self.stations.to_dataframe()
        dataframe = pd.merge(dataframe, stations, left_on='start_station_id', right_index=True, how='left')
        dataframe.to_csv('training.csv')
