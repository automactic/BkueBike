from datarobot import Deployment, PredictionServer

import database


def create_deployment():
    model_id = '5d9d24e95de2e14a72e5b541'
    label = 'BlueBike Ridership -- trip length prediction'
    server = PredictionServer.list()[0]
    deployment = Deployment.create_from_learning_model(
        model_id, label, default_prediction_server_id=server.id
    )
    print(deployment)


if __name__ == '__main__':
    database.create_table_if_not_exist()

    # update data stored in the system
    # stations = Stations()
    # regions = Regions()
    # trips = Trips('data/201908-bluebikes-tripdata.csv')
    # with Dumper() as dumper:
    #     dumper.update_stations(stations, regions)
    #     dumper.update_trip_data(trips)

    # create_deployment()
