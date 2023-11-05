"""
This module contains connector functionality for ThingsBoard.
"""
from dataclasses import dataclass
import json
from tb_rest_client import RestClientCE
from tb_rest_client.models.models_ce import EntityId


@dataclass
class ThingsBoardDevice:
    """Holds the device details from ThingsBoard."""
    name: str
    id: str


@dataclass
class TelemetryMessage:
    """Holds the telemetry message from ThingsBoard."""
    # Battery details
    battery_percentage: int
    battery_timestamp: int
    # GPS details
    gps_latitude: float
    gps_longitude: float
    gps_speed: float
    gps_timestamp: int
    # Development details
    cellular_imei: int
    cellular_iccid: int
    firmware_version: str
    board_version: str
    application_version: str
    development_timestamp: int
    # Environmental details
    environmental_temperature: int
    environmental_humidity: int
    environmental_pressure: int
    environmental_timestamp: int
    # Aritifical intelligence details
    ai_normal_percentage: int
    ai_error1_percentage: int
    ai_error2_percentage: int
    ai_error3_percentage: int
    ai_timestamp: int


class ThingsBoardAdaptor:
    """
    An adaptor layer between the Python applications and the ThingsBoard service. 
    """

    def __init__(self, user_name: str, password: str):
        """
        Initialize the adaptor object.
        """
        self.user_name: str = user_name
        self.password: str = password
        self.thingsboard_client: RestClientCE = RestClientCE("https://thingsboard.cloud")

        # Connect to the ThingsBoard service.
        self.thingsboard_client.login(self.user_name, self.password)
        if not self.thingsboard_client.logged_in:
            raise ValueError("Cannot log in to the service. Check username or password.")

    def get_project_devices(self) -> list[ThingsBoardDevice]:
        """
        Get all devices from ThingsBoard.
        """
        # Find the total number of devices.
        total_elements = self.thingsboard_client.get_tenant_devices(1, 1).total_elements
        total_elements = int(total_elements)

        # Getting all device informations.
        devices = []
        for device_index in range(0, total_elements):
            response = self.thingsboard_client.get_tenant_devices(1, device_index)
            for next_device in response.data:
                if next_device.type == "DieselMotor":
                    devices.append(ThingsBoardDevice(name=str(next_device.name),
                                                     id=str(next_device.id.id)))
        return devices

    def get_device_telemetry(self, device: ThingsBoardDevice) -> list[str]:
        """
        Get all telemetry data from the given device.
        """
        entity = EntityId(id=device.id, entity_type="DEVICE")
        telemetry_message = self.thingsboard_client.get_latest_timeseries(entity)

        return TelemetryMessage(
            battery_percentage=json.loads(telemetry_message["bat"][0]["value"])["v"],
            battery_timestamp=json.loads(telemetry_message["bat"][0]["value"])["ts"],
            gps_latitude=json.loads(telemetry_message["gnss"][0]["value"])["lat"],
            gps_longitude=json.loads(telemetry_message["gnss"][0]["value"])["lng"],
            gps_speed=json.loads(telemetry_message["gnss"][0]["value"])["spd"],
            gps_timestamp=telemetry_message["gnss"][0]["ts"],
            cellular_imei=json.loads(telemetry_message["dev"][0]["value"])["imei"],
            cellular_iccid=json.loads(telemetry_message["dev"][0]["value"])["iccid"],
            firmware_version=json.loads(telemetry_message["dev"][0]["value"])["modV"],
            board_version=json.loads(telemetry_message["dev"][0]["value"])["brdV"],
            application_version=json.loads(telemetry_message["dev"][0]["value"])["appV"],
            development_timestamp=json.loads(telemetry_message["dev"][0]["value"])["ts"],
            environmental_temperature=json.loads(telemetry_message["env"][0]["value"])["temp"],
            environmental_humidity=json.loads(telemetry_message["env"][0]["value"])["hum"],
            environmental_pressure=json.loads(telemetry_message["env"][0]["value"])["atmp"],
            environmental_timestamp=json.loads(telemetry_message["env"][0]["value"])["ts"],
            ai_normal_percentage=json.loads(telemetry_message["ai"][0]["value"])["n"],
            ai_error1_percentage=json.loads(telemetry_message["ai"][0]["value"])["e1"],
            ai_error2_percentage=json.loads(telemetry_message["ai"][0]["value"])["e2"],
            ai_error3_percentage=json.loads(telemetry_message["ai"][0]["value"])["e3"],
            ai_timestamp=json.loads(telemetry_message["ai"][0]["value"])["ts"]
        )
