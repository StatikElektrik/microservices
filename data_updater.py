"""
A microservice to update project database within the ThingsBoard service.
"""
from data_updater.service import DataUpdaterService


if __name__ == '__main__':
    DataUpdaterService(
        database_env=".database.env",
        thingsboard_env=".thingsboard.env"
    ).run()
