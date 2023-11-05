"""
A microservice to update project database within the ThingsBoard service.
"""
from data_updater.thingsboard_adaptor import ThingsBoardAdaptor


# ThingsBoard REST API URL
URL = "https://thingsboard.cloud"
USERNAME = "gokhan.kocmarli@penguen.org.tr"
PASSWORD = "ptaicommon2023"

def main():
    """Main function."""
    connector = ThingsBoardAdaptor(USERNAME, PASSWORD)
    devices = connector.get_project_devices()
    telemetry_messages = []
    for each_device in devices:
        telemetry = connector.get_device_telemetry(devices[0])
        telemetry_messages.append({"device": each_device, "telemetry": telemetry})
    return telemetry_messages

if __name__ == '__main__':
    main()
