from os import listdir
from os.path import isfile, join

import pandas as pd

from data_sources import Regions, Stations


class TrainingData:
    def __init__(self, dir_path='data/training/'):
        self.stations = Stations()
        self.regions = Regions()
        self.dir_path = dir_path

    def _get_file_paths(self):
        files = sorted([join(self.dir_path, file) for file in listdir(self.dir_path)])
        return [file for file in files if isfile(file) and file.endswith('.csv')]

    def process(self):
        stations = self.stations.to_dataframe()
        dataframes = []
        for path in self._get_file_paths():
            dataframe = pd.read_csv(path)
            dataframe = dataframe.rename(columns={
                'tripduration': 'trip_duration',
                'starttime': 'start_time',
                'bikeid': 'bike_id',
                'usertype': 'user_type',
                'start station id': 'start_station_id',
                'birth year': 'birth_year',
                'start station name': 'start_station_name',
                'end station name': 'end_station_name',
            })
            dataframe = dataframe.drop(columns=[
                'stoptime',
                'start station latitude',
                'start station longitude',
                'end station id',
                'end station latitude',
                'end station longitude'
            ])
            dataframe = pd.merge(
                dataframe, stations, left_on='start_station_id', right_index=True, how='left'
            )
            dataframes.append(dataframe)

        dataframe = pd.concat(dataframes)
        dataframe.to_csv('training.csv')
