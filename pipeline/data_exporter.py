import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

import sql
from sql import DatabaseMixin

logger = logging.getLogger(__name__)


class DataExporter(DatabaseMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._engine = self.create_engine()

    def export_trip_data(self, batch_size=10000, csv_path='../data/bluebike_trips_2019.csv'):
        start, end = datetime(2019, 1, 1), datetime(2020, 1, 1)
        conn = self._engine.connect()

        # delete existing file
        path = Path(csv_path)
        if path.exists():
            path.unlink()

        # get total count of rows
        statement = sa.select([
            sa.func.count(sql.trips.c.id).label('count')
        ]).where(sa.and_(
            sql.trips.c.start_time >= start,
            sql.trips.c.start_time < end,
        ))
        total_count = conn.execute(statement).fetchone()['count']

        # export to csv
        for skip in range(0, total_count, batch_size):
            statement = sa.select([
                sql.trips.c.id,
                sql.trips.c.trip_duration,
                sql.trips.c.start_station_id,
                sql.stations.c.name.label('start_station_name'),
                sql.stations.c.latitude.label('start_station_latitude'),
                sql.stations.c.longitude.label('start_station_longitude'),
                sql.stations.c.region_name.label('start_station_region_name'),
                sql.stations.c.capacity.label('start_station_capacity'),
                sql.stations.c.has_kiosk.label('start_station_has_kiosk'),
                sql.trips.c.start_time,
                sql.trips.c.bike_id,
                sql.trips.c.user_type,
                sql.trips.c.user_birth_year,
                sql.trips.c.user_gender,
            ]).select_from(
                sql.trips.join(sql.stations, sql.trips.c.start_station_id == sql.stations.c.id),
            ).where(sa.and_(
                sql.trips.c.start_time >= start,
                sql.trips.c.start_time < end,
            )).order_by(sql.trips.c.start_time).limit(batch_size).offset(skip)
            rows = conn.execute(statement).fetchall()

            # append to csv file
            rows = [dict(row) for row in rows]
            data_frame = pd.DataFrame(rows)
            data_frame.to_csv(csv_path, mode='a', index=False, header=skip == 0)

            # logging
            exported_count = skip + len(rows)
            logger.info(
                f'Exported {exported_count} / {total_count} rows, '
                f'progress: {exported_count/total_count:.2%}'
            )

    def export_station_data(self, csv_path='../data/bluebike_stations.csv'):
        # delete existing file
        path = Path(csv_path)
        if path.exists():
            path.unlink()

        # read data
        conn = self._engine.connect()
        statement = sa.select([sql.stations])
        rows = conn.execute(statement).fetchall()

        # write data
        rows = [dict(row) for row in rows]
        data_frame = pd.DataFrame(rows)
        data_frame.to_csv(csv_path, index=False)

        # logging
        logger.info(f'Exported {len(rows)} stations.')


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    exporter = DataExporter()
    exporter.export_station_data()
    exporter.export_trip_data()
