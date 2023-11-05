"""
This module provides the main service functionality.
"""
from dotenv import dotenv_values
from data_updater.thingsboard_adaptor import ThingsBoardSettings
from data_updater.database_handler import DatabaseSettings

from data_updater.thingsboard_adaptor import ThingsBoardAdaptor, TelemetryMessage, ThingsBoardDevice
from data_updater.database_handler import create_database_handler
from data_updater.database_adaptor import DatabaseAdaptor


class DataUpdaterService:
    """
    This module provides the main service functionality.
    """

    def __init__(self,
                 database_env: str = ".database.env",
                 thingsboard_env: str = ".thingsboard.env"):
        """
        This function initializes the service.
        """
        database_configs: dict = dotenv_values(database_env)
        database_settings = DatabaseSettings(
            name=database_configs['DATABASE_NAME'],
            host=database_configs['DATABASE_HOST'],
            port=database_configs['DATABASE_PORT'],
            username=database_configs['DATABASE_USERNAME'],
            password=database_configs['DATABASE_PASSWORD'],
        )

        thingsboard_configs: dict = dotenv_values(thingsboard_env)
        thingsboard_settings = ThingsBoardSettings(
            url=thingsboard_configs['THINGSBOARD_HOST'],
            username=thingsboard_configs['THINGSBOARD_USERNAME'],
            password=thingsboard_configs['THINGSBOARD_PASSWORD'],
        )

        # Connect to the database.
        database_handler = create_database_handler(database_settings)
        database_handler.connect()
        self.database_adaptor = DatabaseAdaptor(database_handler)

        # Connect to the ThingsBoard service.
        self.thingsboard_connector = ThingsBoardAdaptor(thingsboard_settings)
        self.thingsboard_connector.connect()

    def run(self):
        """Run the service."""
        # Get all telemetry messages from ThingsBoard.
        telemetry_messages = self._get_all_telemetry()
        # Add the telemetry messages to the database.
        for each_telemetry in telemetry_messages:
            self.database_adaptor.create_new_device_table(each_telemetry["device"].name)
            self.database_adaptor.add_telemetry(
                device_id=each_telemetry["device"].name,
                telemetry=each_telemetry["telemetry"]
            )
            self.database_adaptor.update_vehicles_data(
                device_id=each_telemetry["device"].name,
                telemetry=each_telemetry["telemetry"]
            )

    def _get_all_telemetry(self) -> dict[str, ThingsBoardDevice | list[TelemetryMessage]]:
        """Returns all telemetry messages from ThingsBoard for each device."""
        devices = self.thingsboard_connector.get_project_devices()
        telemetry_messages = []
        for each_device in devices:
            telemetry = self.thingsboard_connector.get_device_telemetry(devices[0])
            telemetry_messages.append({"device": each_device, "telemetry": telemetry})
        return telemetry_messages
    