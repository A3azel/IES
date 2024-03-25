from csv import reader
from datetime import datetime

import sys
import config
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData


class FileDatasource:

    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_data = None
        self.gps_data = None
        self.parking_data = None
        pass

    def read(self):
        """Метод повертає дані отримані з датчиків"""
        try:
            accelerometer_data_row = next(self.accelerometer_data)
            gps_data_row = next(self.gps_data)
            parking_data_row = next(self.parking_data)
        except StopIteration:
            sys.exit()

        data = AggregatedData(
            Accelerometer(int(accelerometer_data_row[0]), int(accelerometer_data_row[1]), int(accelerometer_data_row[2])),
            Gps(float(gps_data_row[0]), float(gps_data_row[1])),
            Parking(int(parking_data_row[0]), Gps(float(parking_data_row[1]), float(parking_data_row[2]))),
            datetime.now(),
            config.USER_ID
        )
        return data


    def startReading(self):
        """Метод повинен викликатись перед початком читання даних"""
        try:
            accelerometer_file = open(self.accelerometer_filename)
            self.accelerometer_data = reader(accelerometer_file)
            next(self.accelerometer_data)

            gcp_file = open(self.gps_filename)
            self.gps_data = reader(gcp_file)
            next(self.gps_data)

            parking_file = open(self.parking_filename)
            self.parking_data = reader(parking_file)
            next(self.parking_data)
        except FileNotFoundError as e:
            print(f"File not found: {e.filename}")
            sys.exit()


    def stopReading(self):
        """Метод повинен викликатись для закінчення читання даних"""
        if self.accelerometer_data:
            self.accelerometer_data.close()
        if self.gps_data:
            self.gps_data.close()
        if self.parking_data:
            self.parking_data.close()