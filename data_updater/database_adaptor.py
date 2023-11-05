"""
This module contains database functionality for the application.
"""
from enum import Enum, unique, auto
from data_updater.database_handler import DatabaseHandler
from data_updater.thingsboard_adaptor import TelemetryMessage


@unique
class DeviceTableStatus(Enum):
    """
    This class is used to store device table status.
    """
    NEW = auto()
    EXISTING = auto()


class DatabaseAdaptor:
    """
    This module provides a functionality for exchanging delivered 
    message from device to PostgreSQL database.
    """

    def __init__(self, database_handler: DatabaseHandler):
        self.database_handler = database_handler

    @property
    def device_table_column_names(self) -> dict[str, str]:
        """
        This function returns the column names of the device table.
        """
        return {
                "battery_percentage": "FLOAT",
                "battery_timestamp": "BIGINT",
                "gps_latitude": "FLOAT",
                "gps_longitude": "FLOAT",
                "gps_timestamp": "BIGINT",
                "ai_normal_percentage": "INT",
                "ai_error1_percentage": "INT",
                "ai_error2_percentage": "INT",
                "ai_error3_percentage": "INT",
                "ai_timestamp": "BIGINT",
        }

    def telemetry_to_database_entry_converter(self, telemetry: TelemetryMessage) -> dict[str, str]:
        """
        This function converts the telemetry message to a database entry.
        """
        return {
            "battery_percentage": telemetry.battery_percentage,
            "battery_timestamp": telemetry.battery_timestamp,
            "gps_latitude": telemetry.gps_latitude,
            "gps_longitude": telemetry.gps_longitude,
            "gps_timestamp": telemetry.gps_timestamp,
            "ai_normal_percentage": telemetry.ai_normal_percentage,
            "ai_error1_percentage": telemetry.ai_error1_percentage,
            "ai_error2_percentage": telemetry.ai_error2_percentage,
            "ai_error3_percentage": telemetry.ai_error3_percentage,
            "ai_timestamp": telemetry.ai_timestamp,
        }

    def create_new_device_table(self, device_id: str) -> DeviceTableStatus:
        """
        This function creates a new device table in the database.
        """
        table_name = f"device_{device_id}"
        if not self.database_handler.check_table_exists(table_name):
            self.database_handler.create_table(table_name, self.device_table_column_names)
            return DeviceTableStatus.NEW
        return DeviceTableStatus.EXISTING

    def add_telemetry(self, device_id: str, telemetry: TelemetryMessage):
        """
        This function inserts data to the device table.
        """
        table_name = f"device_{device_id}"
        self.database_handler.insert_into_table(
            table_name,
            self.telemetry_to_database_entry_converter(telemetry)
        )
